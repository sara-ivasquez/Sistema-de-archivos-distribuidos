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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
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
	"sync"
	"time"
)

const (
	opPut       = 1
	opGet       = 2
	opDelete    = 3
	opReplicate = 4 // otro DataNode le pide a este que copie un bloque hacia un tercero
	opPutRaw    = 5 // usado SOLO entre DataNodes, para recibir esa copia

	statusOK    = 1
	statusError = 0
)

var (
	dnHost            string
	dnPort            int
	dnPublicHost      string
	storageDir        string
	capacity          int64
	controlNodeURLs   []string
	activeURLIndex    int
	activeURLMu       sync.Mutex
	heartbeatInterval time.Duration
)

var (
	caCertFile   string
	secureClient *http.Client
)

var encryptionKey []byte

func newSecureHTTPClient(timeout time.Duration) *http.Client {
	caCert, err := os.ReadFile(caCertFile)
	if err != nil {
		log.Printf("[DataNode %d] no se pudo leer el certificado: %v", dnPort, err)
		return &http.Client{Timeout: timeout}
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	return &http.Client{
		Timeout:   timeout,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}},
	}
}

func pickControlNodeURL() string {
	activeURLMu.Lock()
	defer activeURLMu.Unlock()
	return controlNodeURLs[activeURLIndex]
}

func markURLFailed() {
	activeURLMu.Lock()
	activeURLIndex = (activeURLIndex + 1) % len(controlNodeURLs)
	activeURLMu.Unlock()
}

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

func deriveKey(passphrase string) []byte {
	sum := sha256.Sum256([]byte(passphrase))
	return sum[:]
}

func encryptBlock(key []byte, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func decryptBlock(key []byte, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("bloque cifrado invalido (muy corto)")
	}
	nonce, data := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, data, nil)
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
		encrypted, err := encryptBlock(encryptionKey, data)
		if err != nil {
			_ = sendResponse(conn, statusError, []byte(err.Error()))
			return
		}
		if err := os.WriteFile(blockPath(blockID), encrypted, 0644); err != nil {
			_ = sendResponse(conn, statusError, []byte(err.Error()))
			return
		}
		log.Printf("[DataNode %d] PUT %s (%d bytes, cifrado en reposo)", dnPort, blockID, dataLen)
		_ = sendResponse(conn, statusOK, []byte("ok"))

		case opGet:
		encrypted, err := os.ReadFile(blockPath(blockID))
		if err != nil {
			_ = sendResponse(conn, statusError, []byte("bloque no encontrado"))
			return
		}
		data, err := decryptBlock(encryptionKey, encrypted)
		if err != nil {
			_ = sendResponse(conn, statusError, []byte("no se pudo descifrar el bloque: "+err.Error()))
			return
		}
		log.Printf("[DataNode %d] GET %s (%d bytes)", dnPort, blockID, len(data))
		_ = sendResponse(conn, statusOK, data)

	case opDelete:
		_ = os.Remove(blockPath(blockID)) // ignorar si no existe
		log.Printf("[DataNode %d] DELETE %s", dnPort, blockID)
		_ = sendResponse(conn, statusOK, []byte("ok"))

	case opReplicate:
		// El ControlNode nos pide que copiemos un bloque que ya tenemos
		// hacia otro DataNode (target_host:target_port). Hito 3.
		hostLenBuf := make([]byte, 2)
		if err := readFull(conn, hostLenBuf); err != nil {
			return
		}
		hostLen := binary.BigEndian.Uint16(hostLenBuf)
		hostBytes := make([]byte, hostLen)
		if err := readFull(conn, hostBytes); err != nil {
			return
		}
		portBuf := make([]byte, 2)
		if err := readFull(conn, portBuf); err != nil {
			return
		}
		targetPort := int(binary.BigEndian.Uint16(portBuf))
		targetHost := string(hostBytes)

		data, err := os.ReadFile(blockPath(blockID))
		if err != nil {
			_ = sendResponse(conn, statusError, []byte("no tengo ese bloque localmente"))
			return
		}
		if err := putRawRemote(targetHost, targetPort, blockID, data); err != nil {
			log.Printf("[DataNode %d] REPLICATE %s -> %s:%d fallo: %v", dnPort, blockID, targetHost, targetPort, err)
			_ = sendResponse(conn, statusError, []byte(err.Error()))
			return
		}
		log.Printf("[DataNode %d] REPLICATE %s -> %s:%d OK", dnPort, blockID, targetHost, targetPort)
		_ = sendResponse(conn, statusOK, []byte("ok"))

	case opPutRaw:
		// Recibe una copia de un bloque enviada por OTRO DataNode (no por
		// el cliente). Se guarda igual que un PUT normal.
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
		log.Printf("[DataNode %d] PUT_RAW %s (%d bytes, replica P2P recibida)", dnPort, blockID, dataLen)
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

// putRawRemote conecta a otro DataNode y le envia PUT_RAW con los bytes de
// un bloque (usado por REPLICATE para copiar un bloque hacia otro nodo).
func putRawRemote(host string, port int, blockID string, data []byte) error {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 10*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	idBytes := []byte(blockID)
	header := make([]byte, 5)
	header[0] = opPutRaw
	binary.BigEndian.PutUint32(header[1:], uint32(len(idBytes)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(idBytes); err != nil {
		return err
	}
	lenBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBuf, uint64(len(data)))
	if _, err := conn.Write(lenBuf); err != nil {
		return err
	}
	if _, err := conn.Write(data); err != nil {
		return err
	}

	respHeader := make([]byte, 5)
	if err := readFull(conn, respHeader); err != nil {
		return err
	}
	if respHeader[0] != statusOK {
		payloadLen := binary.BigEndian.Uint32(respHeader[1:])
		payload := make([]byte, payloadLen)
		_ = readFull(conn, payload)
		return fmt.Errorf("PUT_RAW fallo: %s", string(payload))
	}
	return nil
}

// ----------------------------------------------------------------------------
// Registro y heartbeat ante el ControlNode (REST/JSON)
// ----------------------------------------------------------------------------

func registerWithControlNode() (string, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"host": dnPublicHost, "port": dnPort, "capacity": capacity,
	})
	var lastErr error
	for i := 0; i < len(controlNodeURLs); i++ {
		url := pickControlNodeURL()
		resp, err := secureClient.Post(url+"/datanodes/register", "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			markURLFailed()
			continue
		}
		if resp.StatusCode != 200 {
			respBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("registro fallo (%d): %s", resp.StatusCode, string(respBody))
			markURLFailed()
			continue
		}
		var out struct {
			DatanodeID string `json:"datanode_id"`
		}
		err = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			markURLFailed()
			continue
		}
		log.Printf("[DataNode %d] registrado ante ControlNode (%s) como %s", dnPort, url, out.DatanodeID)
		return out.DatanodeID, nil
	}
	return "", lastErr
}

func heartbeatLoop(datanodeID string) {
	client := secureClient
	for {
		used, count := currentUsage()
		body, _ := json.Marshal(map[string]interface{}{
			"datanode_id": datanodeID, "used": used, "num_blocks": count,
		})
		ok := false
		for i := 0; i < len(controlNodeURLs); i++ {
			url := pickControlNodeURL()
			resp, err := client.Post(url+"/datanodes/heartbeat", "application/json", bytes.NewReader(body))
			if err != nil {
				markURLFailed()
				continue
			}
			resp.Body.Close()
			ok = true
			break
		}
		if !ok {
			log.Printf("[DataNode %d] heartbeat fallo en todos los ControlNodes", dnPort)
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
	controlNodeURLs = strings.Split(getEnv("CONTROLNODE_URLS", getEnv("CONTROLNODE_URL", "https://127.0.0.1:5000")), ",")
	heartbeatInterval = time.Duration(getEnvInt("DN_HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second
	caCertFile = getEnv("CA_CERT_FILE", "../certs/cert.pem")
	encryptionKey = deriveKey(getEnv("DN_ENCRYPTION_KEY", "clave-compartida-dfsha-2026"))
	secureClient = newSecureHTTPClient(10 * time.Second)

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
