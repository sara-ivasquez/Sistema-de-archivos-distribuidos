// DataNode de DFSha (Hito 2), reescrito en Go por requisito del profesor.
//
// Implementa el MISMO protocolo binario de bloques que common/blockproto.py
// (usado por el cliente en Python) y que controlnode_go/main.go usa para
// pedir DeleteBlock durante "rm". Por eso el cliente Python no necesita
// ningun cambio: solo habla TCP con este programa en vez de con el DataNode
// en Python.
//
// Formato de un REQUEST (big-endian):
//   1 byte  -> opcode (1=PUT, 2=GET, 3=DELETE)
//   4 bytes -> longitud del block_id (uint32)
//   N bytes -> block_id (utf-8)
//   [solo PUT] 8 bytes -> longitud de los datos (uint64)
//   [solo PUT] M bytes -> datos crudos del bloque
// Formato de una RESPONSE:
//   1 byte  -> status (1=ok, 0=error)
//   4 bytes -> longitud del payload (uint32)
//   K bytes -> payload (bytes del bloque en GET, o mensaje de texto)
//
// Ejecucion:
//   go run main.go
// Variables de entorno (iguales a la version Python, ver datanode/server.py):
//   DN_HOST, DN_PORT, DN_PUBLIC_HOST, DN_STORAGE_DIR, DN_CAPACITY,
//   CONTROLNODE_URL, DN_HEARTBEAT_INTERVAL_SECONDS
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	opPut    = 1
	opGet    = 2
	opDelete = 3

	statusOK    = 1
	statusError = 0
)

var (
	dnHost              string
	dnPort              int
	dnPublicHost        string
	storageDir          string
	capacity            int64
	controlNodeURL      string
	heartbeatInterval   time.Duration
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
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

func getEnvInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func blockPath(blockID string) string {
	safe := strings.ReplaceAll(blockID, "/", "_")
	safe = strings.ReplaceAll(safe, "..", "_")
	return filepath.Join(storageDir, safe)
}

func currentUsage() (int64, int) {
	entries, err := os.ReadDir(storageDir)
	if err != nil {
		return 0, 0
	}
	var used int64
	count := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		used += info.Size()
		count++
	}
	return used, count
}

// ----------------------------------------------------------------------------
// Servidor TCP de bloques
// ----------------------------------------------------------------------------

func readFull(conn net.Conn, buf []byte) error {
	_, err := io.ReadFull(conn, buf)
	return err
}

func sendResponse(conn net.Conn, status byte, payload []byte) error {
	header := make([]byte, 5)
	header[0] = status
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	reqHeader := make([]byte, 5)
	if err := readFull(conn, reqHeader); err != nil {
		return
	}
	opcode := reqHeader[0]
	blockIDLen := binary.BigEndian.Uint32(reqHeader[1:])
	blockIDBytes := make([]byte, blockIDLen)
	if err := readFull(conn, blockIDBytes); err != nil {
		return
	}
	blockID := string(blockIDBytes)

	switch opcode {
	case opPut:
		lenBuf := make([]byte, 8)
		if err := readFull(conn, lenBuf); err != nil {
			return
		}
		dataLen := binary.BigEndian.Uint64(lenBuf)
		data := make([]byte, dataLen)
		if dataLen > 0 {
			if err := readFull(conn, data); err != nil {
				return
			}
		}
		if err := os.WriteFile(blockPath(blockID), data, 0644); err != nil {
			_ = sendResponse(conn, statusError, []byte(err.Error()))
			return
		}
		log.Printf("[DataNode %d] PUT %s (%d bytes)", dnPort, blockID, dataLen)
		_ = sendResponse(conn, statusOK, []byte("ok"))

	case opGet:
		data, err := os.ReadFile(blockPath(blockID))
		if err != nil {
			_ = sendResponse(conn, statusError, []byte("bloque no encontrado"))
			return
		}
		log.Printf("[DataNode %d] GET %s (%d bytes)", dnPort, blockID, len(data))
		_ = sendResponse(conn, statusOK, data)

	case opDelete:
		_ = os.Remove(blockPath(blockID)) // ignorar si no existe
		log.Printf("[DataNode %d] DELETE %s", dnPort, blockID)
		_ = sendResponse(conn, statusOK, []byte("ok"))

	default:
		_ = sendResponse(conn, statusError, []byte("opcode desconocido"))
	}
}

func serveBlocks() {
	addr := fmt.Sprintf("%s:%d", dnHost, dnPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[DataNode %d] no se pudo escuchar en %s: %v", dnPort, addr, err)
	}
	log.Printf("[DataNode %d] escuchando bloques en %s (storage=%s)", dnPort, addr, storageDir)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[DataNode %d] error aceptando conexion: %v", dnPort, err)
			continue
		}
		go handleConn(conn)
	}
}

// ----------------------------------------------------------------------------
// Registro y heartbeat ante el ControlNode (REST/JSON)
// ----------------------------------------------------------------------------

func registerWithControlNode() (string, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"host": dnPublicHost, "port": dnPort, "capacity": capacity,
	})
	resp, err := http.Post(controlNodeURL+"/datanodes/register", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("registro fallo (%d): %s", resp.StatusCode, string(respBody))
	}
	var out struct {
		DatanodeID string `json:"datanode_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	log.Printf("[DataNode %d] registrado ante ControlNode como %s", dnPort, out.DatanodeID)
	return out.DatanodeID, nil
}

func heartbeatLoop(datanodeID string) {
	client := &http.Client{Timeout: 5 * time.Second}
	for {
		used, count := currentUsage()
		body, _ := json.Marshal(map[string]interface{}{
			"datanode_id": datanodeID, "used": used, "num_blocks": count,
		})
		resp, err := client.Post(controlNodeURL+"/datanodes/heartbeat", "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("[DataNode %d] heartbeat fallo: %v", dnPort, err)
		} else {
			resp.Body.Close()
		}
		time.Sleep(heartbeatInterval)
	}
}

// ----------------------------------------------------------------------------
// main
// ----------------------------------------------------------------------------

func main() {
	dnHost = getEnv("DN_HOST", "0.0.0.0")
	dnPort = getEnvInt("DN_PORT", 6001)
	dnPublicHost = getEnv("DN_PUBLIC_HOST", "127.0.0.1")
	storageDir = getEnv("DN_STORAGE_DIR", "./storage")
	capacity = getEnvInt64("DN_CAPACITY", 1*1024*1024*1024)
	controlNodeURL = getEnv("CONTROLNODE_URL", "http://127.0.0.1:5000")
	heartbeatInterval = time.Duration(getEnvInt("DN_HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second

	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("[DataNode %d] no se pudo crear storage dir %s: %v", dnPort, storageDir, err)
	}

	datanodeID, err := registerWithControlNode()
	if err != nil {
		log.Fatalf("[DataNode %d] no se pudo registrar ante el ControlNode: %v", dnPort, err)
	}
	go heartbeatLoop(datanodeID)

	serveBlocks()
}
