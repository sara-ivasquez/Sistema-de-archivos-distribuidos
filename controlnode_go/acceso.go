// RF3: servicio de ACCESO a archivos (open, read, write, lock, close).
//
// A diferencia del servicio de TRANSFERENCIA (put/get = send/receive, que
// mueven el archivo completo), aqui el cliente abre un archivo, lee o
// modifica rangos de bytes y lo cierra. Los bytes siguen viajando directo
// entre el cliente y los DataNodes; el ControlNode solo coordina:
//
//   - open: entrega un "handle" con la lista de bloques del archivo
//     (version actual). Leer = pedir a los DataNodes solo los bloques del
//     rango, sin pasar por el ControlNode.
//   - write: el cliente arma en memoria los bloques modificados y, al hacer
//     flush/close, los sube como bloques NUEVOS (write/alloc). El commit
//     reemplaza de forma atomica los bloques viejos por los nuevos
//     (copy-on-write): nadie ve nunca un archivo a medio escribir.
//   - lock: un solo escritor (lock exclusivo, que toma automaticamente un
//     open en modo "rw") y multiples lectores. Un lector puede tomar un lock
//     compartido para impedir que alguien escriba mientras lee.
//   - Los handles (y sus locks) tienen un lease: si el cliente no da senales
//     de vida en HANDLE_TTL_SECONDS, el handle expira y libera el lock. Asi un
//     cliente caido no deja un archivo bloqueado para siempre.
//   - Los bloques reemplazados no se borran de inmediato si hay otros
//     handles abiertos sobre el archivo: se guardan unos segundos para que
//     esos lectores terminen de leer su version (consistencia
//     "close-to-open", como en NFS/AFS).
//
// Los handles, locks y bloques pendientes forman parte del estado que se
// replica al standby, asi que un archivo abierto sobrevive al failover.
package main

import (
	"log"
	"net/http"
	"path"
	"time"
)

// Handle es un archivo abierto por un cliente.
type Handle struct {
	ID       string    `json:"id"`
	Path     string    `json:"path"`
	Owner    string    `json:"owner"`
	Mode     string    `json:"mode"` // "r" | "rw"
	Lock     string    `json:"lock"` // "" | "shared" | "exclusive"
	Pending  []Block   `json:"pending,omitempty"`
	LastSeen time.Time `json:"last_seen"`
}

// GarbageBlock es un bloque que ya no pertenece a ningun archivo pero que se
// borrara recien despues de After (ver arriba: lectores con version vieja).
type GarbageBlock struct {
	Block Block     `json:"block"`
	After time.Time `json:"after"`
}

var (
	handles   = map[string]*Handle{} // protegido por mu
	garbage   []GarbageBlock         // protegido por mu
	handleTTL time.Duration
	gcDelay   time.Duration
)

// ----------------------------------------------------------------------------
// Helpers (todos se llaman con mu tomado)
// ----------------------------------------------------------------------------

// lockConflict dice si alguien distinto de except tiene un lock sobre path
// incompatible con el lock "want".
func lockConflict(p, except, want string) bool {
	for _, h := range handles {
		if h.Path != p || h.ID == except || h.Lock == "" {
			continue
		}
		if want == "exclusive" || h.Lock == "exclusive" {
			return true
		}
	}
	return false
}

func hasOpenHandles(p string) bool {
	for _, h := range handles {
		if h.Path == p {
			return true
		}
	}
	return false
}

// discardBlocks decide que hacer con bloques que dejan de pertenecer a p:
// si hay handles abiertos se borran mas tarde; si no, se devuelven para
// borrarlos ya (fuera del lock).
func discardBlocks(p string, blocks []Block) []Block {
	if len(blocks) == 0 {
		return nil
	}
	if !hasOpenHandles(p) {
		return blocks
	}
	after := time.Now().Add(gcDelay)
	for _, b := range blocks {
		garbage = append(garbage, GarbageBlock{Block: b, After: after})
	}
	return nil
}

// liveFirst ordena las replicas de cada bloque poniendo primero las vivas.
func liveFirst(blocks []Block) []Block {
	alive := aliveDatanodes()
	out := make([]Block, len(blocks))
	for i, b := range blocks {
		reps := append([]Replica{}, b.Replicas...)
		for x := 1; x < len(reps); x++ { // insertion sort estable
			for y := x; y > 0; y-- {
				_, ay := alive[reps[y].DataNodeID]
				_, ap := alive[reps[y-1].DataNodeID]
				if ay && !ap {
					reps[y], reps[y-1] = reps[y-1], reps[y]
				}
			}
		}
		b.Replicas = reps
		out[i] = b
	}
	return out
}

// getHandle valida que el handle exista, sea del usuario y no haya expirado;
// ademas renueva su lease.
func getHandle(w http.ResponseWriter, id, user string) *Handle {
	h, ok := handles[id]
	if !ok || h.Owner != user {
		writeError(w, 404, "handle invalido o expirado (vuelve a hacer open)")
		return nil
	}
	h.LastSeen = time.Now()
	return h
}

func numBlocksFor(size, bs int64) int {
	if size <= 0 {
		return 0
	}
	return int((size + bs - 1) / bs)
}

// ----------------------------------------------------------------------------
// Handlers
// ----------------------------------------------------------------------------

func handleOpen(w http.ResponseWriter, r *http.Request) {
	user, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	var body struct {
		Path   string
		Mode   string
		Create bool
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, 400, "body invalido")
		return
	}
	if body.Mode == "" {
		body.Mode = "r"
	}
	if body.Mode != "r" && body.Mode != "rw" {
		writeError(w, 400, "mode debe ser 'r' o 'rw'")
		return
	}
	p := normPath(body.Path)

	mu.Lock()
	defer mu.Unlock()
	entry, exists := fsTree[p]
	if !exists {
		if body.Mode != "rw" || !body.Create {
			writeError(w, 404, "archivo no encontrado")
			return
		}
		parentPath := normPath(path.Dir(p))
		parent, ok := fsTree[parentPath]
		if !ok || parent.Type != "dir" {
			writeError(w, 404, "el directorio "+parentPath+" no existe")
			return
		}
		if !canAccess(parent, user) {
			writeError(w, 403, "no tienes permiso sobre "+parentPath)
			return
		}
		entry = &FSEntry{Type: "file", BlockSize: blockSize, Status: "complete", Owner: user, CreatedAt: time.Now()}
		fsTree[p] = entry
		parent.Children[p] = true
	}
	if entry.Type != "file" {
		writeError(w, 400, "no es un archivo")
		return
	}
	if entry.Owner != user {
		writeError(w, 403, "no tienes permiso sobre "+p)
		return
	}
	if entry.Status != "complete" {
		writeError(w, 409, "el archivo aun no termino de subirse")
		return
	}
	lock := ""
	if body.Mode == "rw" {
		if lockConflict(p, "", "exclusive") {
			writeError(w, 423, "el archivo esta bloqueado por otro proceso")
			return
		}
		lock = "exclusive"
	}
	h := &Handle{ID: randHex(16), Path: p, Owner: user, Mode: body.Mode, Lock: lock, LastSeen: time.Now()}
	handles[h.ID] = h
	writeJSON(w, 200, map[string]interface{}{
		"handle": h.ID, "mode": h.Mode, "lock": h.Lock, "version": entry.Version,
		"size": entry.Size, "block_size": entry.BlockSize, "blocks": liveFirst(entry.Blocks),
		"lease_seconds": int(handleTTL.Seconds()),
	})
}

func handleLock(w http.ResponseWriter, r *http.Request) {
	user, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	var body struct{ Handle, Type string }
	if err := decodeJSON(r, &body); err != nil || (body.Type != "shared" && body.Type != "exclusive") {
		writeError(w, 400, "type debe ser 'shared' o 'exclusive'")
		return
	}
	mu.Lock()
	defer mu.Unlock()
	h := getHandle(w, body.Handle, user)
	if h == nil {
		return
	}
	if h.Mode == "rw" {
		writeJSON(w, 200, map[string]string{"lock": h.Lock}) // ya tiene el exclusivo
		return
	}
	if lockConflict(h.Path, h.ID, body.Type) {
		writeError(w, 423, "el archivo esta bloqueado por otro proceso")
		return
	}
	h.Lock = body.Type
	writeJSON(w, 200, map[string]string{"lock": h.Lock})
}

func handleUnlock(w http.ResponseWriter, r *http.Request) {
	user, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	var body struct{ Handle string }
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, 400, "body invalido")
		return
	}
	mu.Lock()
	defer mu.Unlock()
	h := getHandle(w, body.Handle, user)
	if h == nil {
		return
	}
	if h.Mode == "rw" {
		writeError(w, 400, "un archivo abierto en 'rw' conserva el lock exclusivo hasta close")
		return
	}
	h.Lock = ""
	writeJSON(w, 200, map[string]string{"lock": ""})
}

// handleRenew mantiene vivo el lease de un handle (el SDK lo llama solo).
func handleRenew(w http.ResponseWriter, r *http.Request) {
	user, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	var body struct{ Handle string }
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, 400, "body invalido")
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if h := getHandle(w, body.Handle, user); h != nil {
		writeJSON(w, 200, map[string]string{"lock": h.Lock})
	}
}

// handleWriteAlloc reserva bloques nuevos (id + replicas) para los indices
// que el cliente modifico. El cliente los escribe directo en los DataNodes.
func handleWriteAlloc(w http.ResponseWriter, r *http.Request) {
	user, ok := requireAuth(r)
	if !ok {
		writeError(w, 401, "no autenticado")
		return
	}
	var body struct {
		Handle  string
		Indices []int
	}
	if err := decodeJSON(r, &body); err != nil || len(body.Indices) == 0 {
		writeError(w, 400, "indices requeridos")
		return
	}
	var toDelete []Block
	defer func() {
		if len(toDelete) > 0 {
			go deleteBlocksRemote(toDelete)
		}
	}()

	mu.Lock()
	defer mu.Unlock()
	h := getHandle(w, body.Handle, user)
	if h == nil {
		return
	}
	if h.Mode != "rw" {
		writeError(w, 403, "el archivo se abrio solo para lectura")
		return
	}
	if len(aliveDatanodes()) == 0 {
		writeError(w, 503, "no hay DataNodes disponibles")
		return
	}
	out := make([]Block, 0, len(body.Indices))
	for _, idx := range body.Indices {
		if idx < 0 || idx > 1<<20 {
			writeError(w, 400, "indice de bloque invalido")
			return
		}
		replicas := pickReplicas(idx)
		if len(replicas) == 0 {
			writeError(w, 503, "no hay DataNodes disponibles")
			return
		}
		blk := Block{BlockID: randHex(16), Index: idx, Replicas: replicas}
		// Si ese indice ya tenia un bloque pendiente (dos flush seguidos sin
		// commit), el anterior se descarta.
		kept := h.Pending[:0]
		for _, pb := range h.Pending {
			if pb.Index == idx {
				toDelete = append(toDelete, pb)
			} else {
				kept = append(kept, pb)
			}
		}
		h.Pending = append(kept, blk)
		out = append(out, blk)
	}
	q := writeQuorum
	if q > len(out[0].Replicas) {
		q = len(out[0].Replicas)
	}
	writeJSON(w, 200, map[string]interface{}{"blocks": out, "write_quorum": q})
}

// commitHandle aplica (atomicamente) los bloques pendientes del handle al
// archivo. Se llama con mu tomado. Devuelve los bloques a borrar ya.
func commitHandle(w http.ResponseWriter, h *Handle, size int64, checksums map[string]string) ([]Block, bool) {
	entry, ok := fsTree[h.Path]
	if !ok || entry.Type != "file" {
		writeError(w, 404, "el archivo ya no existe")
		return nil, false
	}
	if len(h.Pending) == 0 && size == entry.Size {
		return nil, true // nada que escribir
	}
	if size < entry.Size {
		writeError(w, 400, "write no puede reducir el tamano del archivo")
		return nil, false
	}
	bs := entry.BlockSize
	if bs <= 0 {
		bs = blockSize
		entry.BlockSize = bs
	}
	n := numBlocksFor(size, bs)
	newBlocks := make([]Block, n)
	filled := make([]bool, n)
	for _, b := range entry.Blocks {
		if b.Index < n {
			newBlocks[b.Index] = b
			filled[b.Index] = true
		}
	}
	var replaced []Block
	for _, pb := range h.Pending {
		if pb.Index >= n {
			replaced = append(replaced, pb) // escrito y luego fuera de rango: sobra
			continue
		}
		cs, has := checksums[pb.BlockID]
		if !has {
			writeError(w, 400, "falta el checksum de un bloque escrito")
			return nil, false
		}
		pb.Checksum = cs
		if filled[pb.Index] {
			replaced = append(replaced, newBlocks[pb.Index])
		}
		newBlocks[pb.Index] = pb
		filled[pb.Index] = true
	}
	for i := 0; i < n; i++ {
		if !filled[i] {
			writeError(w, 400, "faltan bloques: el cliente debe escribir todos los bloques nuevos")
			return nil, false
		}
	}
	entry.Blocks = newBlocks
	entry.Size = size
	entry.Version++
	h.Pending = nil
	return discardBlocks(h.Path, replaced), true
}

func handleFlushOrClose(closeIt bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := requireAuth(r)
		if !ok {
			writeError(w, 401, "no autenticado")
			return
		}
		var body struct {
			Handle    string
			Size      *int64
			Checksums map[string]string
		}
		if err := decodeJSON(r, &body); err != nil {
			writeError(w, 400, "body invalido")
			return
		}
		var toDelete []Block
		defer func() {
			if len(toDelete) > 0 {
				go deleteBlocksRemote(toDelete)
			}
		}()

		mu.Lock()
		defer mu.Unlock()
		h := getHandle(w, body.Handle, user)
		if h == nil {
			return
		}
		if h.Mode == "rw" && body.Size != nil {
			del, ok := commitHandle(w, h, *body.Size, body.Checksums)
			if !ok {
				return
			}
			toDelete = del
		}
		version := 0
		var size int64
		if e, ok := fsTree[h.Path]; ok {
			version, size = e.Version, e.Size
		}
		if closeIt {
			toDelete = append(toDelete, h.Pending...) // escrito sin commit: se descarta
			delete(handles, h.ID)
		}
		writeJSON(w, 200, map[string]interface{}{"ok": true, "version": version, "size": size})
	}
}

// expireHandlesAndGC se llama periodicamente desde el primario: libera
// handles sin lease y devuelve los bloques que ya se pueden borrar.
func expireHandlesAndGC() []Block {
	mu.Lock()
	defer mu.Unlock()
	now := time.Now()
	var out []Block
	for id, h := range handles {
		if now.Sub(h.LastSeen) > handleTTL {
			log.Printf("[ControlNode] handle %s sobre %s expiro (lock %q liberado)", id, h.Path, h.Lock)
			out = append(out, h.Pending...)
			delete(handles, id)
		}
	}
	kept := garbage[:0]
	for _, g := range garbage {
		if now.After(g.After) {
			out = append(out, g.Block)
		} else {
			kept = append(kept, g)
		}
	}
	garbage = kept
	return out
}
