// ControlNode de DFSha — servidor de metadatos (Go, solo libreria estandar).
//
// Responsabilidades:
//   - Autenticacion de usuarios y sesiones con expiracion (RF4).
//   - Arbol de directorios/archivos y control de acceso por dueno (RF1).
//   - Registro y heartbeat de DataNodes.
//   - Algoritmo de particionamiento y colocacion de bloques (RNF4).
//   - El ControlNode nunca mueve los bytes del archivo (RF2): solo indica
//     donde estan/deben ir; para "rm" y la re-replicacion les da ordenes a
//     los DataNodes por la API de bloques.
//
// Hito 3: HA primario/standby con epocas (sin split-brain), replicacion de
// estado y sesiones, re-replicacion automatica, control de acceso por
// usuario, TLS en todas las comunicaciones y secreto compartido entre nodos.
//
// Ejecucion:
//
//	go run main.go
//
// Variables de entorno:
//
//	CN_HOST (default 0.0.0.0), CN_PORT (default 5000)
//	BLOCK_SIZE (default 262144 = 256 KB)
//	REPLICATION_FACTOR (default 2), WRITE_QUORUM (default N/2+1)
//	HEARTBEAT_TIMEOUT_SECONDS (default 15)
//	CN_ROLE (primary|standby), CN_PEER_URL (https://otro-controlnode:puerto)
//	CERT_FILE, KEY_FILE (default ../certs/cert.pem y ../certs/key.pem)
//	CLUSTER_SECRET (secreto compartido con DataNodes y el otro ControlNode)
//	TOKEN_TTL_HOURS (default 8), UPLOAD_TIMEOUT_MINUTES (default 30)
//	CN_STATE_FILE (default ./controlnode_state.json)
//	HANDLE_TTL_SECONDS (default 60), GC_DELAY_SECONDS (default 60)  [RF3]
package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ----------------------------------------------------------------------------
// Configuracion (leida de variables de entorno con valores por defecto)
// ----------------------------------------------------------------------------

var (
	blockSize         int64
	replicationFactor int
	writeQuorum       int
	heartbeatTimeout  time.Duration
	uploadTimeout     time.Duration
	stateFile         string
)

var (
	role           string // "primary" | "standby"
	configuredRole string // rol pedido por CN_ROLE (solo se usa para desempatar)
	epoch          uint64 // "epoca" del primario; sube en cada promocion (evita split-brain)
	peerURL        string
	roleMu         sync.RWMutex
)

var (
	certFile      string
	keyFile       string
	clusterSecret string
	tokenTTL      time.Duration
	blockTLS      *tls.Config // config TLS para hablar el protocolo de bloques con los DataNodes
)

// Header que deben enviar los nodos internos (DataNodes y el otro
// ControlNode) para que el ControlNode acepte sus solicitudes.
const clusterSecretHeader = "X-DFSha-Cluster-Secret"

func getRole() string {
	roleMu.RLock()
	defer roleMu.RUnlock()
	return role
}

func getEpoch() uint64 {
	roleMu.RLock()
	defer roleMu.RUnlock()
	return epoch
}

func setRole(r string) {
	roleMu.Lock()
	role = r
	roleMu.Unlock()
}

// promote convierte este nodo en primario con una epoca nueva, mayor que
// cualquiera que haya visto. Asi, si el primario viejo revive, puede saber
// que ya no manda.
func promote() {
	roleMu.Lock()
	role = "primary"
	epoch++
	roleMu.Unlock()
}

func setEpochIfHigher(e uint64) {
	roleMu.Lock()
	if e > epoch {
		epoch = e
	}
	roleMu.Unlock()
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// ----------------------------------------------------------------------------
// Modelos y estado en memoria (protegido por un unico mutex; se replica al
// standby y se persiste en disco, ver mas abajo)
// ----------------------------------------------------------------------------

type Replica struct {
	DataNodeID string `json:"datanode_id"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
}

type Block struct {
	BlockID  string    `json:"block_id"`
	Index    int       `json:"index"`
	Replicas []Replica `json:"replicas"`
	Checksum string    `json:"checksum,omitempty"`
}

type FSEntry struct {
	Type      string          `json:"type"` // "dir" | "file"
	Children  map[string]bool `json:"-"`
	Owner     string          `json:"-"`
	Size      int64           `json:"size,omitempty"`
	BlockSize int64           `json:"block_size,omitempty"`
	Blocks    []Block         `json:"blocks,omitempty"`
	Status    string          `json:"status,omitempty"`
	CreatedAt time.Time       `json:"-"`
	Version   int             `json:"-"` // sube en cada commit de RF3 (write)
}

type DataNodeInfo struct {
	Host      string
	Port      int
	Capacity  int64
	Used      int64
	NumBlocks int
	LastHB    time.Time
}

type FSEntrySnapshot struct {
	Type      string    `json:"type"`
	Children  []string  `json:"children,omitempty"`
	Owner     string    `json:"owner,omitempty"`
	Size      int64     `json:"size,omitempty"`
	BlockSize int64     `json:"block_size,omitempty"`
	Blocks    []Block   `json:"blocks,omitempty"`
	Status    string    `json:"status,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	Version   int       `json:"version,omitempty"`
}

type DataNodeSnapshot struct {
	Host      string    `json:"host"`
	Port      int       `json:"port"`
	Capacity  int64     `json:"capacity"`
	Used      int64     `json:"used"`
	NumBlocks int       `json:"num_blocks"`
	LastHB    time.Time `json:"last_hb"`
}

// Sesion de un usuario. Se replica al standby para que, tras un failover,
// el cliente no tenga que volver a hacer login (transparencia).
type TokenInfo struct {
	Username string    `json:"username"`
	Expires  time.Time `json:"expires"`
}

type StateSnapshot struct {
	Epoch     uint64                      `json:"epoch"`
	Users     map[string]string           `json:"users"`
	Tokens    map[string]TokenInfo        `json:"tokens"`
	FSTree    map[string]FSEntrySnapshot  `json:"fs_tree"`
	DataNodes map[string]DataNodeSnapshot `json:"datanodes"`
	Handles   map[string]Handle           `json:"handles"`
	Garbage   []GarbageBlock              `json:"garbage,omitempty"`
}

func exportState() StateSnapshot {
	mu.Lock()
	defer mu.Unlock()
	snap := StateSnapshot{
		Epoch:     getEpoch(),
		Users:     map[string]string{},
		Tokens:    map[string]TokenInfo{},
		FSTree:    map[string]FSEntrySnapshot{},
		DataNodes: map[string]DataNodeSnapshot{},
		Handles:   map[string]Handle{},
		Garbage:   append([]GarbageBlock{}, garbage...),
	}
	for id, h := range handles {
		hc := *h
		hc.Pending = append([]Block{}, h.Pending...)
		snap.Handles[id] = hc
	}
	for u, h := range users {
		snap.Users[u] = h
	}
	for t, info := range tokens {
		snap.Tokens[t] = info
	}
	for p, e := range fsTree {
		children := make([]string, 0, len(e.Children))
		for c := range e.Children {
			children = append(children, c)
		}
		snap.FSTree[p] = FSEntrySnapshot{
			Type: e.Type, Children: children, Owner: e.Owner,
			Size: e.Size, BlockSize: e.BlockSize, Blocks: e.Blocks, Status: e.Status,
			CreatedAt: e.CreatedAt, Version: e.Version,
		}
	}
	for id, dn := range datanodes {
		snap.DataNodes[id] = DataNodeSnapshot{
			Host: dn.Host, Port: dn.Port, Capacity: dn.Capacity,
			Used: dn.Used, NumBlocks: dn.NumBlocks, LastHB: dn.LastHB,
		}
	}
	return snap
}

func importState(snap StateSnapshot) {
	mu.Lock()
	defer mu.Unlock()
	newUsers := map[string]string{}
	for u, h := range snap.Users {
		newUsers[u] = h
	}
	users = newUsers

	newTokens := map[string]TokenInfo{}
	for t, info := range snap.Tokens {
		newTokens[t] = info
	}
	tokens = newTokens

	newFSTree := map[string]*FSEntry{}
	for p, e := range snap.FSTree {
		children := map[string]bool{}
		for _, c := range e.Children {
			children[c] = true
		}
		newFSTree[p] = &FSEntry{
			Type: e.Type, Children: children, Owner: e.Owner,
			Size: e.Size, BlockSize: e.BlockSize, Blocks: e.Blocks, Status: e.Status,
			CreatedAt: e.CreatedAt, Version: e.Version,
		}
	}
	fsTree = newFSTree

	newHandles := map[string]*Handle{}
	for id, h := range snap.Handles {
		hc := h
		newHandles[id] = &hc
	}
	handles = newHandles
	garbage = append([]GarbageBlock{}, snap.Garbage...)

	newDatanodes := map[string]*DataNodeInfo{}
	for id, d := range snap.DataNodes {
		newDatanodes[id] = &DataNodeInfo{
			Host: d.Host, Port: d.Port, Capacity: d.Capacity,
			Used: d.Used, NumBlocks: d.NumBlocks, LastHB: d.LastHB,
		}
	}
	datanodes = newDatanodes
}

var (
	mu        sync.Mutex
	users     = map[string]string{}    // username -> hash con sal del password
	tokens    = map[string]TokenInfo{} // token -> sesion
	datanodes = map[string]*DataNodeInfo{}
	fsTree    = map[string]*FSEntry{
		"/": {Type: "dir", Children: map[string]bool{}},
	}
)

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// Contrasenas: sal aleatoria + SHA-256 iterado (formato "s1$<sal>$<hash>").
// Asi dos usuarios con la misma clave no tienen el mismo hash y un ataque
// de diccionario cuesta passwordIterations veces mas.
const passwordIterations = 100000

func hashPassword(password, salt string) string {
	sum := sha256.Sum256([]byte(salt + password))
	for i := 1; i < passwordIterations; i++ {
		sum = sha256.Sum256(append([]byte(salt), sum[:]...))
	}
	return "s1$" + salt + "$" + hex.EncodeToString(sum[:])
}

func checkPassword(stored, password string) bool {
	var expected string
	if parts := strings.SplitN(stored, "$", 3); len(parts) == 3 && parts[0] == "s1" {
		expected = hashPassword(password, parts[1])
	} else {
		expected = sha256Hex(password) // formato del Hito 2 (sin sal)
	}
	return subtle.ConstantTimeCompare([]byte(stored), []byte(expected)) == 1
}

func randHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func normPath(p string) string {
	if p == "" {
		p = "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	cleaned := path.Clean(p)
	if cleaned == "." {
		cleaned = "/"
	}
	return cleaned
}

// ----------------------------------------------------------------------------
// Utilidades HTTP (helpers para no repetir codigo en cada handler)
// ----------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func requireAuth(r *http.Request) (string, bool) {
	authz := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(authz) <= len(prefix) || authz[:len(prefix)] != prefix {
		return "", false
	}
	token := authz[len(prefix):]
	mu.Lock()
	defer mu.Unlock()
	info, ok := tokens[token]
	if !ok {
		return "", false
	}
	if time.Now().After(info.Expires) {
		delete(tokens, token)
		return "", false
	}
	return info.Username, true
}

// requireClusterSecret protege los endpoints que solo deben usar otros
// nodos del cluster (registro/heartbeat de DataNodes y sincronizacion entre
// ControlNodes). Sin esto, cualquiera podria registrar un DataNode falso o
// reemplazar todo el estado del ControlNode.
func requireClusterSecret(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get(clusterSecretHeader)
		if subtle.ConstantTimeCompare([]byte(got), []byte(clusterSecret)) != 1 {
			writeError(w, 403, "nodo no autorizado")
			return
		}
		handler(w, r)
	}
}

// canAccess: un usuario solo puede operar sobre lo que le pertenece. La
// raiz "/" (owner vacio) es compartida: ahi cualquiera puede crear, pero
// solo ve y toca sus propias entradas.
func canAccess(entry *FSEntry, user string) bool {
	return entry.Owner == "" || entry.Owner == user
}

func decodeJSON(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

// ----------------------------------------------------------------------------
// Algoritmo de particionamiento y colocacion (RNF4)
// ----------------------------------------------------------------------------

func aliveDatanodes() map[string]*DataNodeInfo {
	now := time.Now()
	alive := map[string]*DataNodeInfo{}
	for id, dn := range datanodes {
		if now.Sub(dn.LastHB) < heartbeatTimeout {
			alive[id] = dn
		}
	}
	return alive
}

func pickReplicas(blockIndex int) []Replica {
	alive := aliveDatanodes()
	if len(alive) == 0 {
		return nil
	}
	type kv struct {
		id string
		dn *DataNodeInfo
	}
	ordered := make([]kv, 0, len(alive))
	for id, dn := range alive {
		ordered = append(ordered, kv{id, dn})
	}
	sort.Slice(ordered, func(i, j int) bool {
		li := float64(ordered[i].dn.Used) / float64(maxInt64(ordered[i].dn.Capacity, 1))
		lj := float64(ordered[j].dn.Used) / float64(maxInt64(ordered[j].dn.Capacity, 1))
		return li < lj
	})
	n := len(ordered)
	start := blockIndex % n
	rotated := append(append([]kv{}, ordered[start:]...), ordered[:start]...)
	k := replicationFactor
	if k > n {
		k = n
	}
	out := make([]Replica, 0, k)
	for i := 0; i < k; i++ {
		out = append(out, Replica{DataNodeID: rotated[i].id, Host: rotated[i].dn.Host, Port: rotated[i].dn.Port})
	}
	return out
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ----------------------------------------------------------------------------
// Auth (RF4)
// ----------------------------------------------------------------------------

func handleRegister(w http.ResponseWriter, r *http.Request) {
	var body struct{ Username, Password string }
	if err := decodeJSON(r, &body); err != nil || body.Username == "" || body.Password == "" {
		writeError(w, 400, "username y password son requeridos")
		return
	}
	if len(body.Username) > 64 || len(body.Password) < 4 {
		writeError(w, 400, "usuario de maximo 64 caracteres y password de minimo 4")
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if _, exists := users[body.Username]; exists {
		writeError(w, 409, "el usuario ya existe")
		return
	}
	users[body.Username] = hashPassword(body.Password, randHex(16))
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	var body struct{ Username, Password string }
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, 400, "body invalido")
		return
	}
	mu.Lock()
	stored, ok := users[body.Username]
	mu.Unlock()
	if !ok || !checkPassword(stored, body.Password) {
		writeError(w, 401, "credenciales invalidas")
		return
	}
	mu.Lock()
	defer mu.Unlock()
	token := randHex(32)
	tokens[token] = TokenInfo{Username: body.Username, Expires: time.Now().Add(tokenTTL)}
	writeJSON(w, 200, map[string]string{"token": token})
}

// ----------------------------------------------------------------------------
// Gestion del sistema de archivos (RF1)
// ----------------------------------------------------------------------------

func handleMkdir(w http.ResponseWriter, r *http.Request) {
	owner, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	var body struct{ Path string }
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, 400, "body invalido")
		return
	}
	p := normPath(body.Path)
	parent := normPath(path.Dir(p))

	mu.Lock()
	defer mu.Unlock()
	parentEntry, exists := fsTree[parent]
	if !exists || parentEntry.Type != "dir" {
		writeError(w, 404, fmt.Sprintf("el directorio padre %s no existe", parent))
		return
	}
	if !canAccess(parentEntry, owner) {
		writeError(w, 403, "no tienes permiso sobre "+parent)
		return
	}
	if _, exists := fsTree[p]; exists {
		writeError(w, 409, "la ruta ya existe")
		return
	}
	fsTree[p] = &FSEntry{Type: "dir", Children: map[string]bool{}, Owner: owner}
	parentEntry.Children[p] = true
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func handleLs(w http.ResponseWriter, r *http.Request) {
	user, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	p := normPath(r.URL.Query().Get("path"))

	mu.Lock()
	defer mu.Unlock()
	node, exists := fsTree[p]
	if !exists || node.Type != "dir" {
		writeError(w, 404, "directorio no encontrado")
		return
	}
	if !canAccess(node, user) {
		writeError(w, 403, "no tienes permiso sobre "+p)
		return
	}
	names := make([]string, 0, len(node.Children))
	for child := range node.Children {
		names = append(names, child)
	}
	sort.Strings(names)

	type entry struct {
		Name   string `json:"name"`
		Type   string `json:"type"`
		Size   int64  `json:"size"` // sin omitempty: un archivo de 0 bytes tambien trae "size"
		Status string `json:"status,omitempty"`
	}
	entries := make([]entry, 0, len(names))
	for _, child := range names {
		c := fsTree[child]
		if c.Owner != user {
			continue // en la raiz compartida solo se ven las entradas propias
		}
		e := entry{Name: path.Base(child), Type: c.Type}
		if c.Type == "file" {
			e.Size = c.Size
			e.Status = c.Status
		}
		entries = append(entries, e)
	}
	writeJSON(w, 200, map[string]interface{}{"entries": entries})
}

func handleRmdir(w http.ResponseWriter, r *http.Request) {
	user, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	p := normPath(r.URL.Query().Get("path"))
	if p == "/" {
		writeError(w, 400, "no se puede eliminar el directorio raiz")
		return
	}

	mu.Lock()
	defer mu.Unlock()
	node, exists := fsTree[p]
	if !exists || node.Type != "dir" {
		writeError(w, 404, "directorio no encontrado")
		return
	}
	if node.Owner != user {
		writeError(w, 403, "no tienes permiso sobre "+p)
		return
	}
	if len(node.Children) > 0 {
		writeError(w, 400, "el directorio no esta vacio")
		return
	}
	parent := normPath(path.Dir(p))
	delete(fsTree[parent].Children, p)
	delete(fsTree, p)
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func handleRm(w http.ResponseWriter, r *http.Request) {
	user, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	p := normPath(r.URL.Query().Get("path"))

	mu.Lock()
	node, exists := fsTree[p]
	if !exists || node.Type != "file" {
		mu.Unlock()
		writeError(w, 404, "archivo no encontrado")
		return
	}
	if node.Owner != user {
		mu.Unlock()
		writeError(w, 403, "no tienes permiso sobre "+p)
		return
	}
	if lockConflict(p, "", "exclusive") {
		mu.Unlock()
		writeError(w, 423, "el archivo esta bloqueado por otro proceso")
		return
	}
	blocks := discardBlocks(p, node.Blocks)
	parent := normPath(path.Dir(p))
	delete(fsTree[parent].Children, p)
	delete(fsTree, p)
	mu.Unlock()

	// Fuera del lock: avisar a los DataNodes (best-effort, API de bloques)
	deleteBlocksRemote(blocks)
	writeJSON(w, 200, map[string]bool{"ok": true})
}

// deleteBlocksRemote pide a cada replica que borre su copia, en paralelo
// para que un DataNode caido (timeout) no frene al resto.
func deleteBlocksRemote(blocks []Block) {
	var wg sync.WaitGroup
	for _, b := range blocks {
		for _, rep := range b.Replicas {
			wg.Add(1)
			go func(host string, port int, id string) {
				defer wg.Done()
				_ = deleteBlockRemote(host, port, id)
			}(rep.Host, rep.Port, b.BlockID)
		}
	}
	wg.Wait()
}

// ----------------------------------------------------------------------------
// Transferencia de archivos (RF2) — particionamiento y colocacion (RNF4)
// ----------------------------------------------------------------------------

func handlePutInit(w http.ResponseWriter, r *http.Request) {
	owner, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	var body struct {
		Path string
		Size int64
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, 400, "body invalido")
		return
	}
	p := normPath(body.Path)
	parent := normPath(path.Dir(p))
	if p == "/" || body.Size < 0 {
		writeError(w, 400, "ruta o tamano invalido")
		return
	}

	// Bloques de una version anterior del archivo (si se sobrescribe). Se
	// borran de los DataNodes fuera del lock, al final.
	var oldBlocks []Block
	defer func() {
		if len(oldBlocks) > 0 {
			go deleteBlocksRemote(oldBlocks)
		}
	}()

	mu.Lock()
	defer mu.Unlock()
	parentEntry, exists := fsTree[parent]
	if !exists || parentEntry.Type != "dir" {
		writeError(w, 404, fmt.Sprintf("el directorio %s no existe", parent))
		return
	}
	if !canAccess(parentEntry, owner) {
		writeError(w, 403, "no tienes permiso sobre "+parent)
		return
	}
	if existing, exists := fsTree[p]; exists {
		if existing.Type == "dir" {
			writeError(w, 409, "ya existe un directorio con ese nombre")
			return
		}
		if existing.Owner != owner {
			writeError(w, 403, "no tienes permiso sobre "+p)
			return
		}
		if lockConflict(p, "", "exclusive") {
			writeError(w, 423, "el archivo esta bloqueado por otro proceso")
			return
		}
	}
	if len(aliveDatanodes()) == 0 {
		writeError(w, 503, "no hay DataNodes disponibles")
		return
	}

	numBlocks := int((body.Size + blockSize - 1) / blockSize)
	if numBlocks < 1 {
		numBlocks = 1
	}
	blocks := make([]Block, 0, numBlocks)
	for i := 0; i < numBlocks; i++ {
		replicas := pickReplicas(i)
		if len(replicas) == 0 {
			writeError(w, 503, "no hay DataNodes disponibles")
			return
		}
		blocks = append(blocks, Block{BlockID: randHex(16), Index: i, Replicas: replicas})
	}

	if existing, exists := fsTree[p]; exists {
		oldBlocks = discardBlocks(p, existing.Blocks) // sobrescritura: la version vieja se borra
	}
	fsTree[p] = &FSEntry{
		Type: "file", Size: body.Size, BlockSize: blockSize,
		Blocks: blocks, Status: "uploading", Owner: owner, CreatedAt: time.Now(),
	}
	parentEntry.Children[p] = true

	effectiveQuorum := writeQuorum
	if effectiveQuorum > len(blocks[0].Replicas) {
		effectiveQuorum = len(blocks[0].Replicas)
	}
	writeJSON(w, 200, map[string]interface{}{
		"file_id": p, "block_size": blockSize, "blocks": blocks, "write_quorum": effectiveQuorum,
	})
}

func handlePutComplete(w http.ResponseWriter, r *http.Request) {
	user, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	var body struct {
		Path      string
		Checksums map[string]string
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, 400, "body invalido")
		return
	}
	p := normPath(body.Path)

	mu.Lock()
	defer mu.Unlock()
	node, exists := fsTree[p]
	if !exists || node.Type != "file" {
		writeError(w, 404, "archivo no encontrado")
		return
	}
	if node.Owner != user {
		writeError(w, 403, "no tienes permiso sobre "+p)
		return
	}
	node.Status = "complete"
	for i := range node.Blocks {
		if cs, has := body.Checksums[node.Blocks[i].BlockID]; has {
			node.Blocks[i].Checksum = cs
		}
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func handleGetFile(w http.ResponseWriter, r *http.Request) {
	user, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	p := normPath(r.URL.Query().Get("path"))

	mu.Lock()
	defer mu.Unlock()
	node, exists := fsTree[p]
	if !exists || node.Type != "file" {
		writeError(w, 404, "archivo no encontrado")
		return
	}
	if node.Owner != user {
		writeError(w, 403, "no tienes permiso sobre "+p)
		return
	}
	if node.Status != "complete" {
		writeError(w, 409, "el archivo aun no termino de subirse")
		return
	}
	// Se devuelven las replicas vivas primero, para que el cliente no pierda
	// tiempo (timeouts) intentando leer de un DataNode caido.
	alive := aliveDatanodes()
	blocks := make([]Block, len(node.Blocks))
	for i, b := range node.Blocks {
		reps := append([]Replica{}, b.Replicas...)
		sort.SliceStable(reps, func(x, y int) bool {
			_, ax := alive[reps[x].DataNodeID]
			_, ay := alive[reps[y].DataNodeID]
			return ax && !ay
		})
		b.Replicas = reps
		blocks[i] = b
	}
	writeJSON(w, 200, map[string]interface{}{
		"size": node.Size, "block_size": node.BlockSize, "blocks": blocks,
	})
}

// ----------------------------------------------------------------------------
// Registro y heartbeat de DataNodes
// ----------------------------------------------------------------------------

func handleDatanodeRegister(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Host       string
		Port       int
		Capacity   int64
		DatanodeID string `json:"datanode_id"`
	}
	if err := decodeJSON(r, &body); err != nil || body.Host == "" || body.Port <= 0 {
		writeError(w, 400, "body invalido")
		return
	}
	if body.Capacity == 0 {
		body.Capacity = 10 * 1024 * 1024 * 1024
	}
	// Si el DataNode ya tenia un ID (reinicio, o re-registro tras un
	// failover del ControlNode) lo conservamos: asi sus bloques siguen
	// contando como replicas validas en los metadatos.
	id := body.DatanodeID
	if len(id) < 4 || len(id) > 64 || id[:3] != "dn-" {
		id = "dn-" + randHex(4)
	}

	mu.Lock()
	datanodes[id] = &DataNodeInfo{Host: body.Host, Port: body.Port, Capacity: body.Capacity, LastHB: time.Now()}
	mu.Unlock()

	log.Printf("[ControlNode] DataNode registrado: %s en %s:%d", id, body.Host, body.Port)
	writeJSON(w, 200, map[string]string{"datanode_id": id})
}

func handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DatanodeID string `json:"datanode_id"`
		Used       int64  `json:"used"`
		NumBlocks  int    `json:"num_blocks"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, 400, "body invalido")
		return
	}
	mu.Lock()
	defer mu.Unlock()
	dn, exists := datanodes[body.DatanodeID]
	if !exists {
		writeError(w, 404, "datanode no registrado")
		return
	}
	dn.Used = body.Used
	dn.NumBlocks = body.NumBlocks
	dn.LastHB = time.Now()
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func handleDatanodesStatus(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	now := time.Now()
	out := map[string]interface{}{}
	for id, dn := range datanodes {
		out[id] = map[string]interface{}{
			"host": dn.Host, "port": dn.Port, "capacity": dn.Capacity,
			"used": dn.Used, "num_blocks": dn.NumBlocks,
			"alive": now.Sub(dn.LastHB) < heartbeatTimeout,
		}
	}
	writeJSON(w, 200, out)
}

// ----------------------------------------------------------------------------
// Re-replicacion automatica (Hito 3, RNF2/RNF3): cada 10s revisa si algun
// bloque quedo con menos replicas vivas que REPLICATION_FACTOR (porque un
// DataNode se cayo) y le ordena a un DataNode sano que lo copie hacia otro,
// directamente (P2P), sin que los bytes pasen por el ControlNode.
// ----------------------------------------------------------------------------

// loadCAPool carga el certificado autofirmado del cluster como unica CA de
// confianza. Si falla, el ControlNode no arranca: preferimos caer a hablar
// con otros nodos sin verificar quienes son.
func loadCAPool() *x509.CertPool {
	caCert, err := os.ReadFile(certFile)
	if err != nil {
		log.Fatalf("[ControlNode] no se pudo leer el certificado %s: %v", certFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		log.Fatalf("[ControlNode] %s no contiene un certificado PEM valido", certFile)
	}
	return pool
}

func newSecureHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: loadCAPool(), MinVersion: tls.VersionTLS12}},
	}
}

// postInternal envia un POST a otro nodo del cluster con el secreto compartido.
func postInternal(client *http.Client, url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(clusterSecretHeader, clusterSecret)
	return client.Do(req)
}

func reReplicationLoop() {
	for {
		time.Sleep(10 * time.Second)
		if getRole() != "primary" {
			continue // el standby solo observa; si actuara, ambos copiarian lo mismo
		}

		type fix struct {
			path       string
			blockIndex int
			blockID    string
			srcHost    string
			srcPort    int
			target     Replica
		}
		var fixes []fix
		var abandoned []Block

		mu.Lock()
		// Subidas abandonadas: un archivo que lleva mucho en "uploading" (el
		// cliente aborto o se cayo) se elimina junto con sus bloques.
		for p, entry := range fsTree {
			if entry.Type != "file" || entry.Status != "uploading" {
				continue
			}
			if entry.CreatedAt.IsZero() {
				entry.CreatedAt = time.Now()
				continue
			}
			if time.Since(entry.CreatedAt) > uploadTimeout {
				log.Printf("[ControlNode] eliminando subida abandonada %s", p)
				abandoned = append(abandoned, entry.Blocks...)
				if parent, ok := fsTree[normPath(path.Dir(p))]; ok {
					delete(parent.Children, p)
				}
				delete(fsTree, p)
			}
		}
		alive := aliveDatanodes()
		for p, entry := range fsTree {
			if entry.Type != "file" || entry.Status != "complete" {
				continue
			}
			for bi := range entry.Blocks {
				blk := &entry.Blocks[bi]
				have := map[string]bool{}
				var aliveReplicas []Replica
				for _, rep := range blk.Replicas {
					have[rep.DataNodeID] = true
					if _, ok := alive[rep.DataNodeID]; ok {
						aliveReplicas = append(aliveReplicas, rep)
					}
				}
				if len(aliveReplicas) == 0 || len(aliveReplicas) >= replicationFactor {
					continue // ya esta completo, o no queda ninguna copia de donde sacarlo
				}
				var target *Replica
				for id, dn := range alive {
					if !have[id] {
						target = &Replica{DataNodeID: id, Host: dn.Host, Port: dn.Port}
						break
					}
				}
				if target == nil {
					continue // no hay un DataNode libre para recibir la nueva replica
				}
				src := aliveReplicas[0]
				fixes = append(fixes, fix{
					path: p, blockIndex: bi, blockID: blk.BlockID,
					srcHost: src.Host, srcPort: src.Port, target: *target,
				})
			}
		}
		mu.Unlock()

		abandoned = append(abandoned, expireHandlesAndGC()...)
		if len(abandoned) > 0 {
			deleteBlocksRemote(abandoned)
		}
		for _, f := range fixes {
			log.Printf("[ControlNode] re-replicando bloque %s (%s, indice %d): %s:%d -> %s:%d",
				f.blockID, f.path, f.blockIndex, f.srcHost, f.srcPort, f.target.Host, f.target.Port)
			if err := replicateBlockRemote(f.srcHost, f.srcPort, f.blockID, f.target.Host, f.target.Port); err != nil {
				log.Printf("[ControlNode] re-replicacion de %s fallo: %v", f.blockID, err)
				continue
			}
			mu.Lock()
			if entry, ok := fsTree[f.path]; ok && f.blockIndex < len(entry.Blocks) {
				entry.Blocks[f.blockIndex].Replicas = append(entry.Blocks[f.blockIndex].Replicas, f.target)
			}
			mu.Unlock()
		}
	}
}

// ----------------------------------------------------------------------------
// Cliente de la API de bloques (TCP + TLS) para DELETE y REPLICATE. El
// framing esta documentado en datanode_go/main.go y common/blockproto.py.
// ----------------------------------------------------------------------------

const (
	opDelete    = 3
	opReplicate = 4
)

// dialBlock abre una conexion TLS hacia un DataNode, verificando su
// certificado contra la CA del cluster.
func dialBlock(host string, port int, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	cfg := blockTLS.Clone()
	cfg.ServerName = host
	conn, err := tls.DialWithDialer(dialer, "tcp", fmt.Sprintf("%s:%d", host, port), cfg)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	return conn, nil
}

// blockHeader arma el inicio de un request del protocolo de bloques. Las
// operaciones administrativas (DELETE, REPLICATE, PUT_RAW) llevan ademas el
// secreto del cluster: solo nodos internos pueden borrar o copiar bloques.
func blockHeader(opcode byte, blockID string, withSecret bool) []byte {
	idBytes := []byte(blockID)
	buf := make([]byte, 5, 5+len(idBytes)+2+len(clusterSecret))
	buf[0] = opcode
	binary.BigEndian.PutUint32(buf[1:], uint32(len(idBytes)))
	buf = append(buf, idBytes...)
	if withSecret {
		secretLen := make([]byte, 2)
		binary.BigEndian.PutUint16(secretLen, uint16(len(clusterSecret)))
		buf = append(buf, secretLen...)
		buf = append(buf, []byte(clusterSecret)...)
	}
	return buf
}

func deleteBlockRemote(host string, port int, blockID string) error {
	conn, err := dialBlock(host, port, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.Write(blockHeader(opDelete, blockID, true)); err != nil {
		return err
	}

	respHeader := make([]byte, 5)
	if _, err := readFull(conn, respHeader); err != nil {
		return err
	}
	payloadLen := binary.BigEndian.Uint32(respHeader[1:])
	if payloadLen > 0 {
		payload := make([]byte, payloadLen)
		if _, err := readFull(conn, payload); err != nil {
			return err
		}
	}
	return nil
}

// replicateBlockRemote le ordena al DataNode (host,port) que ya tiene el
// bloque blockID que lo copie directamente (P2P) hacia (targetHost,targetPort).
// Esta es la pieza central de la re-replicacion automatica (Hito 3).
func replicateBlockRemote(host string, port int, blockID, targetHost string, targetPort int) error {
	conn, err := dialBlock(host, port, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	hostBytes := []byte(targetHost)
	buf := blockHeader(opReplicate, blockID, true)
	hostLen := make([]byte, 2)
	binary.BigEndian.PutUint16(hostLen, uint16(len(hostBytes)))
	buf = append(buf, hostLen...)
	buf = append(buf, hostBytes...)
	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, uint16(targetPort))
	buf = append(buf, portBytes...)

	if _, err := conn.Write(buf); err != nil {
		return err
	}

	respHeader := make([]byte, 5)
	if _, err := readFull(conn, respHeader); err != nil {
		return err
	}
	status := respHeader[0]
	payloadLen := binary.BigEndian.Uint32(respHeader[1:])
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := readFull(conn, payload); err != nil {
			return err
		}
	}
	if status != 1 {
		return fmt.Errorf("replicate fallo: %s", string(payload))
	}
	return nil
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// ----------------------------------------------------------------------------
// Alta disponibilidad: roles, replicacion de estado y watchdog
//
// Esquema primario/standby con "epocas" para evitar split-brain:
//   - Cada vez que un nodo se promueve a primario, sube su epoca.
//   - El standby solo acepta snapshots de una epoca >= a la suya, y un nodo
//     que es primario NUNCA acepta un snapshot (antes, un primario viejo que
//     revivia con estado vacio podia borrar todo el estado del nuevo).
//   - Al arrancar, un nodo le pregunta al peer su rol: si el peer ya es
//     primario, arranca como standby aunque CN_ROLE diga "primary".
//   - Si aun asi quedan dos primarios (p. ej. una particion de red), el de
//     epoca menor detecta el rechazo del otro y se degrada a standby.
// ----------------------------------------------------------------------------

type peerStatus struct {
	Role  string `json:"role"`
	Epoch uint64 `json:"epoch"`
}

func handleInternalHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, peerStatus{Role: getRole(), Epoch: getEpoch()})
}

func handleInternalSync(w http.ResponseWriter, r *http.Request) {
	var snap StateSnapshot
	if err := decodeJSON(r, &snap); err != nil {
		writeError(w, 400, "snapshot invalido")
		return
	}
	myRole, myEpoch := getRole(), getEpoch()
	if myRole == "primary" || snap.Epoch < myEpoch {
		log.Printf("[ControlNode] snapshot rechazado (soy %s epoca %d, llego epoca %d)", myRole, myEpoch, snap.Epoch)
		writeJSON(w, 409, peerStatus{Role: myRole, Epoch: myEpoch})
		return
	}
	importState(snap)
	setEpochIfHigher(snap.Epoch)
	writeJSON(w, 200, map[string]bool{"ok": true})
}

// shouldStepDown decide si este primario debe degradarse al ver que el peer
// tambien es primario.
func shouldStepDown(peer peerStatus) bool {
	if peer.Role != "primary" {
		return false
	}
	mine := getEpoch()
	if peer.Epoch != mine {
		return peer.Epoch > mine
	}
	// Empate de epocas: cede el que fue configurado como standby.
	return configuredRole == "standby"
}

var (
	syncClient   *http.Client
	syncMu       sync.Mutex
	peerDownLogs int
	peerDown     atomic.Bool // el ultimo intento de sincronizar fallo
)

// pushToPeer envia el estado completo al standby. Se serializa con syncMu y
// cada envio exporta el estado mas reciente, asi que el orden no importa.
func pushToPeer() {
	if peerURL == "" || getRole() != "primary" {
		return
	}
	syncMu.Lock()
	defer syncMu.Unlock()
	body, _ := json.Marshal(exportState())
	resp, err := postInternal(syncClient, peerURL+"/internal/sync", body)
	peerDown.Store(err != nil)
	if err != nil {
		if peerDownLogs%15 == 0 { // no llenar el log mientras el peer esta caido
			log.Printf("[ControlNode] no se pudo sincronizar con %s: %v", peerURL, err)
		}
		peerDownLogs++
		return
	}
	defer resp.Body.Close()
	peerDownLogs = 0
	if resp.StatusCode == 409 {
		var peer peerStatus
		_ = json.NewDecoder(resp.Body).Decode(&peer)
		if shouldStepDown(peer) {
			log.Printf("[ControlNode] *** el peer es primario con epoca %d (la mia es %d): paso a STANDBY ***", peer.Epoch, getEpoch())
			setRole("standby")
			setEpochIfHigher(peer.Epoch)
		}
	} else if resp.StatusCode != 200 {
		log.Printf("[ControlNode] sincronizacion con %s respondio %d", peerURL, resp.StatusCode)
	}
}

// replicationLoop sincroniza periodicamente (cubre cambios que no pasan por
// un handler, como heartbeats o la re-replicacion).
func replicationLoop() {
	for {
		time.Sleep(2 * time.Second)
		pushToPeer()
	}
}

// bufferedWriter guarda la respuesta de un handler en memoria para poder
// enviarla DESPUES de replicar el cambio.
type bufferedWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (b *bufferedWriter) Header() http.Header         { return b.header }
func (b *bufferedWriter) Write(p []byte) (int, error) { return b.body.Write(p) }
func (b *bufferedWriter) WriteHeader(status int)      { b.status = status }

// replicated envuelve los handlers que modifican el estado: si la operacion
// tuvo exito, el cambio se copia al standby y a disco ANTES de responderle
// al cliente (replicacion semi-sincrona). Asi, si el primario cae justo
// despues de responder, el standby ya tiene ese cambio.
func replicated(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buf := &bufferedWriter{header: w.Header(), status: 200}
		handler(buf, r)
		if buf.status < 300 {
			// Si el standby esta caido no se le espera: el ciclo periodico
			// lo pondra al dia cuando vuelva.
			if !peerDown.Load() {
				pushToPeer()
			}
			persistState()
		}
		w.WriteHeader(buf.status)
		_, _ = w.Write(buf.body.Bytes())
	}
}

func queryPeer(client *http.Client) (peerStatus, error) {
	var st peerStatus
	resp, err := client.Get(peerURL + "/internal/health")
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

func watchdogLoop() {
	if peerURL == "" {
		return
	}
	client := newSecureHTTPClient(2 * time.Second)
	failures := 0
	for {
		time.Sleep(3 * time.Second)
		if getRole() != "standby" {
			failures = 0
			continue
		}
		st, err := queryPeer(client)
		switch {
		case err != nil:
			failures++
			log.Printf("[ControlNode] primario no responde (%d/3): %v", failures, err)
		case st.Role != "primary":
			// El peer responde pero tampoco es primario (p. ej. ambos
			// arrancaron como standby): nadie atiende clientes.
			failures++
			log.Printf("[ControlNode] el peer responde pero es %s (%d/3)", st.Role, failures)
		default:
			failures = 0
			setEpochIfHigher(st.Epoch)
		}
		if failures >= 3 {
			promote()
			log.Printf("[ControlNode] *** EL PRIMARIO NO RESPONDE: asumiendo rol de PRIMARIO (epoca %d) ***", getEpoch())
			failures = 0
		}
	}
}

// resolveInitialRole decide el rol de arranque preguntandole al peer.
func resolveInitialRole() {
	if peerURL != "" {
		client := newSecureHTTPClient(2 * time.Second)
		for attempt := 1; attempt <= 3; attempt++ {
			st, err := queryPeer(client)
			if err == nil {
				if st.Role == "primary" {
					if st.Epoch >= epoch {
						if role == "primary" {
							log.Printf("[ControlNode] el peer ya es primario (epoca %d): arranco como STANDBY", st.Epoch)
						}
						role = "standby"
						epoch = st.Epoch
						return
					}
					// Mi estado en disco es mas nuevo que el del peer (p. ej.
					// ambos cayeron y el peer arranco con datos viejos): asumo
					// como primario y el peer se degradara al ver mi epoca.
					log.Printf("[ControlNode] el peer es primario con epoca %d < mi epoca guardada %d: asumo PRIMARIO", st.Epoch, epoch)
					role = "primary"
					return
				}
				break // el peer responde y es standby: respeto CN_ROLE
			}
			time.Sleep(1 * time.Second)
		}
	}
	if role == "primary" && epoch == 0 {
		epoch = 1
	}
}

// ----------------------------------------------------------------------------
// Persistencia de metadatos: el estado (arbol, usuarios, sesiones, epoca) se
// guarda en disco cada pocos segundos, para sobrevivir aunque caigan los dos
// ControlNodes a la vez. Se escribe a un .tmp y se renombra (atomico).
// ----------------------------------------------------------------------------

func loadStateFromDisk() {
	data, err := os.ReadFile(stateFile)
	if err != nil {
		return
	}
	var snap StateSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		log.Printf("[ControlNode] estado en %s ilegible, se ignora: %v", stateFile, err)
		return
	}
	if snap.FSTree["/"].Type != "dir" {
		log.Printf("[ControlNode] estado en %s sin raiz, se ignora", stateFile)
		return
	}
	importState(snap)
	epoch = snap.Epoch
	log.Printf("[ControlNode] estado restaurado de %s (epoca %d, %d entradas)", stateFile, snap.Epoch, len(snap.FSTree))
}

var (
	persistMu   sync.Mutex
	lastPersist []byte
)

func persistState() {
	persistMu.Lock()
	defer persistMu.Unlock()
	data, err := json.Marshal(exportState())
	if err != nil || bytes.Equal(data, lastPersist) {
		return
	}
	tmp := stateFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		log.Printf("[ControlNode] no se pudo guardar el estado: %v", err)
		return
	}
	if err := os.Rename(tmp, stateFile); err != nil {
		log.Printf("[ControlNode] no se pudo guardar el estado: %v", err)
		return
	}
	lastPersist = data
}

func persistLoop() {
	for {
		time.Sleep(3 * time.Second)
		persistState()
	}
}

func requirePrimary(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if getRole() != "primary" {
			writeError(w, 503, "este ControlNode esta en modo standby, no acepta solicitudes")
			return
		}
		handler(w, r)
	}
}

func withMethod(method string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			writeError(w, 405, "metodo no permitido")
			return
		}
		handler(w, r)
	}
}

// ----------------------------------------------------------------------------
// main
// ----------------------------------------------------------------------------

const defaultClusterSecret = "dfsha-cluster-secret-CAMBIAR"

func main() {
	host := getEnv("CN_HOST", "0.0.0.0")
	port := getEnv("CN_PORT", "5000")
	blockSize = getEnvInt64("BLOCK_SIZE", 256*1024)
	replicationFactor = getEnvInt("REPLICATION_FACTOR", 2)
	heartbeatTimeout = time.Duration(getEnvInt("HEARTBEAT_TIMEOUT_SECONDS", 15)) * time.Second
	writeQuorum = getEnvInt("WRITE_QUORUM", replicationFactor/2+1)
	tokenTTL = time.Duration(getEnvInt("TOKEN_TTL_HOURS", 8)) * time.Hour
	uploadTimeout = time.Duration(getEnvInt("UPLOAD_TIMEOUT_MINUTES", 30)) * time.Minute
	stateFile = getEnv("CN_STATE_FILE", "./controlnode_state.json")
	handleTTL = time.Duration(getEnvInt("HANDLE_TTL_SECONDS", 60)) * time.Second
	gcDelay = time.Duration(getEnvInt("GC_DELAY_SECONDS", 60)) * time.Second

	role = getEnv("CN_ROLE", "primary")
	if role != "primary" && role != "standby" {
		log.Fatalf("[ControlNode] CN_ROLE debe ser 'primary' o 'standby' (llego %q)", role)
	}
	configuredRole = role
	peerURL = getEnv("CN_PEER_URL", "")
	certFile = getEnv("CERT_FILE", "../certs/cert.pem")
	keyFile = getEnv("KEY_FILE", "../certs/key.pem")
	clusterSecret = getEnv("CLUSTER_SECRET", defaultClusterSecret)
	if clusterSecret == defaultClusterSecret {
		log.Printf("[ControlNode] AVISO: CLUSTER_SECRET tiene el valor por defecto; cambialo en despliegue real")
	}
	blockTLS = &tls.Config{RootCAs: loadCAPool(), MinVersion: tls.VersionTLS12}

	loadStateFromDisk()
	resolveInitialRole()

	mux := http.NewServeMux()
	mux.HandleFunc("/auth/register", requirePrimary(withMethod("POST", replicated(handleRegister))))
	mux.HandleFunc("/auth/login", requirePrimary(withMethod("POST", replicated(handleLogin))))
	mux.HandleFunc("/fs/mkdir", requirePrimary(withMethod("POST", replicated(handleMkdir))))
	mux.HandleFunc("/fs/ls", requirePrimary(withMethod("GET", handleLs)))
	mux.HandleFunc("/fs/rmdir", requirePrimary(withMethod("DELETE", replicated(handleRmdir))))
	mux.HandleFunc("/fs/rm", requirePrimary(withMethod("DELETE", replicated(handleRm))))
	mux.HandleFunc("/files/put/init", requirePrimary(withMethod("POST", replicated(handlePutInit))))
	mux.HandleFunc("/files/put/complete", requirePrimary(withMethod("POST", replicated(handlePutComplete))))
	mux.HandleFunc("/files/get", requirePrimary(withMethod("GET", handleGetFile)))
	// RF3: acceso a archivos (ver acceso.go)
	mux.HandleFunc("/files/open", requirePrimary(withMethod("POST", replicated(handleOpen))))
	mux.HandleFunc("/files/lock", requirePrimary(withMethod("POST", replicated(handleLock))))
	mux.HandleFunc("/files/unlock", requirePrimary(withMethod("POST", replicated(handleUnlock))))
	mux.HandleFunc("/files/renew", requirePrimary(withMethod("POST", handleRenew)))
	mux.HandleFunc("/files/write/alloc", requirePrimary(withMethod("POST", replicated(handleWriteAlloc))))
	mux.HandleFunc("/files/flush", requirePrimary(withMethod("POST", replicated(handleFlushOrClose(false)))))
	mux.HandleFunc("/files/close", requirePrimary(withMethod("POST", replicated(handleFlushOrClose(true)))))
	mux.HandleFunc("/datanodes/register", requireClusterSecret(requirePrimary(withMethod("POST", replicated(handleDatanodeRegister)))))
	mux.HandleFunc("/datanodes/heartbeat", requireClusterSecret(requirePrimary(withMethod("POST", handleHeartbeat))))
	mux.HandleFunc("/datanodes/status", withMethod("GET", handleDatanodesStatus))
	mux.HandleFunc("/internal/health", withMethod("GET", handleInternalHealth))
	mux.HandleFunc("/internal/sync", requireClusterSecret(withMethod("POST", handleInternalSync)))

	syncClient = newSecureHTTPClient(3 * time.Second)
	go replicationLoop()
	go watchdogLoop()
	go reReplicationLoop()
	go persistLoop()

	addr := host + ":" + port
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("[ControlNode] escuchando en %s (rol=%s, epoca=%d, block_size=%d, replication=%d, TLS=on)",
		addr, getRole(), getEpoch(), blockSize, replicationFactor)
	if err := server.ListenAndServeTLS(certFile, keyFile); err != nil {
		log.Fatal(err)
	}
}
