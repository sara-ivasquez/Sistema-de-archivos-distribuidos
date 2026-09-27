// ControlNode de DFSha (Hito 2), reescrito en Go por requisito del profesor
// (servicio distribuido en Java o Go; el cliente se mantiene en Python).
//
// Usa unicamente la libreria estandar de Go (net/http, encoding/json, etc.)
// para que compile con "go build" sin necesitar "go get" ni internet.
//
// Responsabilidades (identicas a la version en Python, ver controlnode/app.py
// y el Entregable 1 - RF1, RF4, RF5, RNF4):
//   - Autenticacion basica (RF4).
//   - Gestion del arbol de directorios / archivos: mkdir, ls, rmdir, rm (RF1).
//   - Registro y heartbeat de DataNodes.
//   - Algoritmo de particionamiento y colocacion de bloques (RNF4).
//   - El ControlNode nunca mueve los bytes del archivo (RF2): solo indica
//     donde estan/deben ir; para "rm" les pide a los DataNodes que borren
//     sus replicas, usando el mismo protocolo de bloques que usa el cliente.
//
// Ejecucion:
//   go run main.go
// Variables de entorno:
//   CN_HOST (default 0.0.0.0), CN_PORT (default 5000)
//   BLOCK_SIZE (default 262144 = 256 KB)
//   REPLICATION_FACTOR (default 2)
//   HEARTBEAT_TIMEOUT_SECONDS (default 15)
package main

import (
	"crypto/rand"
	"crypto/sha256"
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
	"sync"
	"time"
)

// ----------------------------------------------------------------------------
// Configuracion (leida de variables de entorno con valores por defecto)
// ----------------------------------------------------------------------------

var (
	blockSize         int64
	replicationFactor int
	heartbeatTimeout  time.Duration
)

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
// Modelos y estado en memoria (equivalente a USERS/TOKENS/DATANODES/FS en la
// version Python; protegido por un unico mutex, igual que el LOCK de Flask)
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
}

type FSEntry struct {
	Type      string          `json:"type"` // "dir" | "file"
	Children  map[string]bool `json:"-"`
	Owner     string          `json:"-"`
	Size      int64           `json:"size,omitempty"`
	BlockSize int64           `json:"block_size,omitempty"`
	Blocks    []Block         `json:"blocks,omitempty"`
	Status    string          `json:"status,omitempty"`
}

type DataNodeInfo struct {
	Host      string
	Port      int
	Capacity  int64
	Used      int64
	NumBlocks int
	LastHB    time.Time
}

var (
	mu        sync.Mutex
	users     = map[string]string{} // username -> sha256 hex del password
	tokens    = map[string]string{} // token -> username
	datanodes = map[string]*DataNodeInfo{}
	fsTree    = map[string]*FSEntry{
		"/": {Type: "dir", Children: map[string]bool{}},
	}
)

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
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
	username, ok := tokens[token]
	return username, ok
}

func decodeJSON(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

// ----------------------------------------------------------------------------
// Algoritmo de particionamiento y colocacion (RNF4) — equivalente a
// _pick_replicas() en controlnode/app.py
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
	mu.Lock()
	defer mu.Unlock()
	if _, exists := users[body.Username]; exists {
		writeError(w, 409, "el usuario ya existe")
		return
	}
	users[body.Username] = sha256Hex(body.Password)
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	var body struct{ Username, Password string }
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, 400, "body invalido")
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if hash, ok := users[body.Username]; !ok || hash != sha256Hex(body.Password) {
		writeError(w, 401, "credenciales invalidas")
		return
	}
	token := randHex(16)
	tokens[token] = body.Username
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
	if _, exists := fsTree[p]; exists {
		writeError(w, 409, "la ruta ya existe")
		return
	}
	fsTree[p] = &FSEntry{Type: "dir", Children: map[string]bool{}, Owner: owner}
	parentEntry.Children[p] = true
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func handleLs(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAuth(r); !ok {
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
	names := make([]string, 0, len(node.Children))
	for child := range node.Children {
		names = append(names, child)
	}
	sort.Strings(names)

	type entry struct {
		Name   string `json:"name"`
		Type   string `json:"type"`
		Size   int64  `json:"size,omitempty"`
		Status string `json:"status,omitempty"`
	}
	entries := make([]entry, 0, len(names))
	for _, child := range names {
		c := fsTree[child]
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
	if _, ok := requireAuth(r); !ok {
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
	if _, ok := requireAuth(r); !ok {
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
	blocks := node.Blocks
	parent := normPath(path.Dir(p))
	delete(fsTree[parent].Children, p)
	delete(fsTree, p)
	mu.Unlock()

	// Fuera del lock: avisar a los DataNodes (best-effort, mismo protocolo
	// binario que usa el cliente en common/blockproto.py)
	for _, b := range blocks {
		for _, rep := range b.Replicas {
			_ = deleteBlockRemote(rep.Host, rep.Port, b.BlockID)
		}
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
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

	mu.Lock()
	defer mu.Unlock()
	parentEntry, exists := fsTree[parent]
	if !exists || parentEntry.Type != "dir" {
		writeError(w, 404, fmt.Sprintf("el directorio %s no existe", parent))
		return
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

	fsTree[p] = &FSEntry{
		Type: "file", Size: body.Size, BlockSize: blockSize,
		Blocks: blocks, Status: "uploading", Owner: owner,
	}
	parentEntry.Children[p] = true

	writeJSON(w, 200, map[string]interface{}{
		"file_id": p, "block_size": blockSize, "blocks": blocks,
	})
}

func handlePutComplete(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAuth(r); !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	var body struct{ Path string }
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
	node.Status = "complete"
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func handleGetFile(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAuth(r); !ok {
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
	if node.Status != "complete" {
		writeError(w, 409, "el archivo aun no termino de subirse")
		return
	}
	writeJSON(w, 200, map[string]interface{}{
		"size": node.Size, "block_size": node.BlockSize, "blocks": node.Blocks,
	})
}

// ----------------------------------------------------------------------------
// Registro y heartbeat de DataNodes
// ----------------------------------------------------------------------------

func handleDatanodeRegister(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Host     string
		Port     int
		Capacity int64
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, 400, "body invalido")
		return
	}
	if body.Capacity == 0 {
		body.Capacity = 10 * 1024 * 1024 * 1024
	}
	id := "dn-" + randHex(4)

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
// Cliente del protocolo de bloques (para que ControlNode pida DeleteBlock a
// los DataNodes durante "rm"). Mismo framing binario que common/blockproto.py
// y datanode_go/main.go: 1 byte opcode + 4 bytes len(block_id) + block_id +
// [solo PUT] 8 bytes len(data) + data. Respuesta: 1 byte status + 4 bytes
// len(payload) + payload.
// ----------------------------------------------------------------------------

const opDelete = 3

func deleteBlockRemote(host string, port int, blockID string) error {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	idBytes := []byte(blockID)
	header := make([]byte, 5)
	header[0] = opDelete
	binary.BigEndian.PutUint32(header[1:], uint32(len(idBytes)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(idBytes); err != nil {
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
// main
// ----------------------------------------------------------------------------

func withMethod(method string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			writeError(w, 405, "metodo no permitido")
			return
		}
		handler(w, r)
	}
}

func main() {
	host := getEnv("CN_HOST", "0.0.0.0")
	port := getEnv("CN_PORT", "5000")
	blockSize = getEnvInt64("BLOCK_SIZE", 256*1024)
	replicationFactor = getEnvInt("REPLICATION_FACTOR", 2)
	heartbeatTimeout = time.Duration(getEnvInt("HEARTBEAT_TIMEOUT_SECONDS", 15)) * time.Second

	mux := http.NewServeMux()
	mux.HandleFunc("/auth/register", withMethod("POST", handleRegister))
	mux.HandleFunc("/auth/login", withMethod("POST", handleLogin))
	mux.HandleFunc("/fs/mkdir", withMethod("POST", handleMkdir))
	mux.HandleFunc("/fs/ls", withMethod("GET", handleLs))
	mux.HandleFunc("/fs/rmdir", withMethod("DELETE", handleRmdir))
	mux.HandleFunc("/fs/rm", withMethod("DELETE", handleRm))
	mux.HandleFunc("/files/put/init", withMethod("POST", handlePutInit))
	mux.HandleFunc("/files/put/complete", withMethod("POST", handlePutComplete))
	mux.HandleFunc("/files/get", withMethod("GET", handleGetFile))
	mux.HandleFunc("/datanodes/register", withMethod("POST", handleDatanodeRegister))
	mux.HandleFunc("/datanodes/heartbeat", withMethod("POST", handleHeartbeat))
	mux.HandleFunc("/datanodes/status", withMethod("GET", handleDatanodesStatus))

	addr := host + ":" + port
	log.Printf("[ControlNode] escuchando en %s (block_size=%d, replication=%d)", addr, blockSize, replicationFactor)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
