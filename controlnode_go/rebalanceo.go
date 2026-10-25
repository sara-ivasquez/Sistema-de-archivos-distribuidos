// Rebalanceo de bloques entre DataNodes.
//
// El algoritmo de colocacion (pickReplicas) solo reparte bien los bloques
// NUEVOS. Si se agrega un DataNode al cluster, ese nodo empieza vacio y los
// viejos siguen llenos; si un DataNode caido vuelve despues de que sus
// bloques se re-replicaron, esos bloques quedan con copias de mas.
//
// Cada ronda (cada 10 s, en el primario, solo cuando no hay bloques con
// replicas faltantes) se hace lo siguiente:
//
//  1. Recorte: si un bloque tiene mas replicas vivas que REPLICATION_FACTOR,
//     se quita la copia del DataNode mas cargado.
//  2. Movimiento: mientras la diferencia de bloques entre el DataNode mas
//     cargado y el menos cargado sea mayor que el umbral, se mueve un bloque
//     del primero al segundo. Mover = el origen copia el bloque P2P al
//     destino (REPLICATE), se actualizan los metadatos y la copia del origen
//     se borra mas tarde.
//
// Se hacen como maximo REBALANCE_MAX_MOVES movimientos por ronda para no
// saturar la red. Las copias que salen de los metadatos no se borran de
// inmediato (GC_DELAY_SECONDS), para no romper a un lector que abrio el
// archivo antes del cambio; y justo antes de borrar se verifica que ningun
// archivo vuelva a referenciar esa copia.
package main

import (
	"log"
	"sort"
	"time"
)

var (
	rebalanceEnabled  bool
	rebalanceMaxMoves int
)

type blockRef struct {
	path    string
	index   int
	blockID string
}

type move struct {
	ref      blockRef
	from, to Replica
}

// referencedReplicas devuelve, para cada block_id, los DataNodes que segun
// los metadatos (archivos y bloques pendientes de RF3) tienen una copia.
// Se llama con mu tomado.
func referencedReplicas() map[string]map[string]bool {
	refs := map[string]map[string]bool{}
	add := func(b Block) {
		set := refs[b.BlockID]
		if set == nil {
			set = map[string]bool{}
			refs[b.BlockID] = set
		}
		for _, r := range b.Replicas {
			set[r.DataNodeID] = true
		}
	}
	for _, e := range fsTree {
		for _, b := range e.Blocks {
			add(b)
		}
	}
	for _, h := range handles {
		for _, b := range h.Pending {
			add(b)
		}
	}
	return refs
}

// scheduleReplicaDeletion pone en la lista de basura la copia de un bloque
// en un DataNode concreto. Se llama con mu tomado.
func scheduleReplicaDeletion(blockID string, rep Replica) {
	garbage = append(garbage, GarbageBlock{
		Block: Block{BlockID: blockID, Replicas: []Replica{rep}},
		After: time.Now().Add(gcDelay),
	})
}

// pendingDeletion dice si ya hay un borrado programado de ese bloque en ese
// DataNode (para no mover un bloque a un nodo donde se va a borrar).
func pendingDeletion(blockID, dnID string) bool {
	for _, g := range garbage {
		if g.Block.BlockID != blockID {
			continue
		}
		for _, r := range g.Block.Replicas {
			if r.DataNodeID == dnID {
				return true
			}
		}
	}
	return false
}

// planRebalance decide los recortes (ya aplicados en los metadatos) y los
// movimientos (a ejecutar fuera del lock). Se llama con mu tomado.
func planRebalance() []move {
	alive := aliveDatanodes()
	if len(alive) < 2 {
		return nil
	}
	count := map[string]int{}
	for id := range alive {
		count[id] = 0
	}

	// Archivos que no se tocan: subidas en curso y archivos abiertos para
	// escritura (su lista de bloques esta por cambiar).
	busy := map[string]bool{}
	for _, h := range handles {
		if h.Lock == "exclusive" {
			busy[h.Path] = true
		}
	}

	paths := make([]string, 0, len(fsTree))
	for p := range fsTree {
		paths = append(paths, p)
	}
	sort.Strings(paths) // orden determinista: facil de seguir en los logs

	// 1. Recorte de replicas sobrantes + conteo de bloques por DataNode.
	for _, p := range paths {
		e := fsTree[p]
		if e.Type != "file" || e.Status != "complete" {
			continue
		}
		for bi := range e.Blocks {
			b := &e.Blocks[bi]
			var aliveReps []int
			for ri, r := range b.Replicas {
				if _, ok := alive[r.DataNodeID]; ok {
					aliveReps = append(aliveReps, ri)
				}
			}
			for len(aliveReps) > replicationFactor && !busy[p] {
				// quitar la copia del DataNode mas cargado
				worst := 0
				for k := range aliveReps {
					if count[b.Replicas[aliveReps[k]].DataNodeID] > count[b.Replicas[aliveReps[worst]].DataNodeID] {
						worst = k
					}
				}
				ri := aliveReps[worst]
				rep := b.Replicas[ri]
				log.Printf("[ControlNode] rebalanceo: bloque %s (%s) tenia copias de mas; se quita la de %s", b.BlockID, p, rep.DataNodeID)
				b.Replicas = append(b.Replicas[:ri:ri], b.Replicas[ri+1:]...)
				scheduleReplicaDeletion(b.BlockID, rep)
				aliveReps = nil
				for ri2, r := range b.Replicas {
					if _, ok := alive[r.DataNodeID]; ok {
						aliveReps = append(aliveReps, ri2)
					}
				}
			}
			for _, ri := range aliveReps {
				count[b.Replicas[ri].DataNodeID]++
			}
		}
	}

	// 2. Movimientos del mas cargado al menos cargado.
	total := 0
	for _, c := range count {
		total += c
	}
	threshold := total / len(count) / 10 // 10% del promedio
	if threshold < 1 {
		threshold = 1
	}
	used := map[string]bool{} // un bloque se mueve como mucho una vez por ronda
	var moves []move
	for len(moves) < rebalanceMaxMoves {
		var maxID, minID string
		for id := range count {
			if maxID == "" || count[id] > count[maxID] || (count[id] == count[maxID] && id < maxID) {
				maxID = id
			}
			if minID == "" || count[id] < count[minID] || (count[id] == count[minID] && id < minID) {
				minID = id
			}
		}
		if count[maxID]-count[minID] <= threshold {
			break
		}
		found := false
		for _, p := range paths {
			e := fsTree[p]
			if e.Type != "file" || e.Status != "complete" || busy[p] {
				continue
			}
			for bi, b := range e.Blocks {
				if used[b.BlockID] || pendingDeletion(b.BlockID, minID) {
					continue
				}
				var from *Replica
				hasMin := false
				for i := range b.Replicas {
					switch b.Replicas[i].DataNodeID {
					case maxID:
						from = &b.Replicas[i]
					case minID:
						hasMin = true
					}
				}
				if from == nil || hasMin {
					continue
				}
				dn := alive[minID]
				moves = append(moves, move{
					ref:  blockRef{path: p, index: bi, blockID: b.BlockID},
					from: *from,
					to:   Replica{DataNodeID: minID, Host: dn.Host, Port: dn.Port},
				})
				used[b.BlockID] = true
				count[maxID]--
				count[minID]++
				found = true
				break
			}
			if found {
				break
			}
		}
		if !found {
			break // no hay ningun bloque movible entre esos dos nodos
		}
	}
	return moves
}

// rebalanceRound planea y ejecuta una ronda de rebalanceo.
func rebalanceRound() {
	if !rebalanceEnabled {
		return
	}
	mu.Lock()
	moves := planRebalance()
	mu.Unlock()

	done := 0
	for _, m := range moves {
		if err := replicateBlockRemote(m.from.Host, m.from.Port, m.ref.blockID, m.to.Host, m.to.Port); err != nil {
			log.Printf("[ControlNode] rebalanceo de %s fallo: %v", m.ref.blockID, err)
			continue
		}
		mu.Lock()
		e, ok := fsTree[m.ref.path]
		if ok && m.ref.index < len(e.Blocks) && e.Blocks[m.ref.index].BlockID == m.ref.blockID {
			b := &e.Blocks[m.ref.index]
			for i := range b.Replicas {
				if b.Replicas[i].DataNodeID == m.from.DataNodeID {
					b.Replicas[i] = m.to
					scheduleReplicaDeletion(b.BlockID, m.from)
					done++
					break
				}
			}
		} else {
			// El archivo cambio mientras copiabamos: la copia nueva sobra.
			scheduleReplicaDeletion(m.ref.blockID, m.to)
		}
		mu.Unlock()
	}
	if done > 0 {
		log.Printf("[ControlNode] rebalanceo: %d bloque(s) movidos en esta ronda", done)
	}
}

// safeGarbage filtra de los bloques a borrar las copias que algun archivo
// vuelve a referenciar (p. ej. un bloque que se movio a un nodo y luego
// regreso). Se llama con mu tomado.
func safeGarbage(blocks []Block) []Block {
	if len(blocks) == 0 {
		return nil
	}
	refs := referencedReplicas()
	var out []Block
	for _, b := range blocks {
		var reps []Replica
		for _, r := range b.Replicas {
			if !refs[b.BlockID][r.DataNodeID] {
				reps = append(reps, r)
			}
		}
		if len(reps) > 0 {
			b.Replicas = reps
			out = append(out, b)
		}
	}
	return out
}
