// DataNode de DFSha — servidor de bloques (Go, solo libreria estandar).
//
// Expone dos APIs:
//
//  1. API de bloques (DN_PORT, TCP + TLS): protocolo binario propio, usado
//     por el Cliente (PUT/GET), por el ControlNode (DELETE/REPLICATE) y por
//     otros DataNodes (PUT_RAW, en la re-replicacion P2P).
//  2. API de administracion (DN_API_PORT, HTTPS/REST): GET /health con el
//     estado del nodo (id, uso, capacidad, numero de bloques).
//
// Formato de un REQUEST de bloques (big-endian):
//
//	1 byte  -> opcode (1=PUT, 2=GET, 3=DELETE, 4=REPLICATE, 5=PUT_RAW)
//	4 bytes -> longitud del block_id (uint32)
//	N bytes -> block_id (solo [A-Za-z0-9_-])
//	[DELETE/REPLICATE/PUT_RAW] 2 bytes len(secreto) + secreto del cluster
//	[PUT/PUT_RAW] 8 bytes len(datos) + datos
//	[REPLICATE]   2 bytes len(host destino) + host + 2 bytes puerto destino
//
// Formato de una RESPONSE:
//
//	1 byte  -> status (1=ok, 0=error)
//	4 bytes -> longitud del payload (uint32)
//	K bytes -> payload (bytes del bloque en GET, o mensaje de texto)
//
// Seguridad (Hito 3):
//   - Todo el trafico va cifrado con TLS (certificado del cluster).
//   - Las operaciones administrativas exigen el secreto compartido del
//     cluster: un cliente no puede borrar ni copiar bloques.
//   - Los bloques se guardan cifrados en disco con AES-256-GCM.
//
// Variables de entorno:
//
//	DN_HOST (0.0.0.0), DN_PORT (6001), DN_API_PORT (DN_PORT+1000)
//	DN_PUBLIC_HOST (127.0.0.1): host/IP con el que otros nodos llegan a este
//	DN_STORAGE_DIR (./storage), DN_CAPACITY (1 GB)
//	CONTROLNODE_URLS (https://127.0.0.1:5000,https://127.0.0.1:5001)
//	DN_HEARTBEAT_INTERVAL_SECONDS (5), DN_MAX_BLOCK_BYTES (128 MB)
//	CERT_FILE, KEY_FILE (../certs/cert.pem, ../certs/key.pem)
//	CLUSTER_SECRET, DN_ENCRYPTION_KEY
package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	opPut       = 1
	opGet       = 2
	opDelete    = 3
	opReplicate = 4 // el ControlNode le pide a este nodo que copie un bloque hacia otro
	opPutRaw    = 5 // usado SOLO entre DataNodes, para recibir esa copia

	statusOK    = 1
	statusError = 0

	clusterSecretHeader = "X-DFSha-Cluster-Secret"
	idFileName          = ".datanode_id"
	maxBlockIDLen       = 128
)

var validBlockID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

var (
	dnHost            string
	dnPort            int
	dnAPIPort         int
	dnPublicHost      string
	storageDir        string
	capacity          int64
	maxBlockBytes     uint64
	heartbeatInterval time.Duration

	controlNodeURLs []string
	activeURLIndex  int
	activeURLMu     sync.Mutex

	certFile      string
	keyFile       string
	clusterSecret string
	encryptionKey []byte

	caPool       *x509.CertPool
	secureClient *http.Client

	datanodeID   string
	datanodeIDMu sync.RWMutex
)

// ----------------------------------------------------------------------------
// Utilidades
// ----------------------------------------------------------------------------

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

func getID() string {
	datanodeIDMu.RLock()
	defer datanodeIDMu.RUnlock()
	return datanodeID
}

func setID(id string) {
	datanodeIDMu.Lock()
	datanodeID = id
	datanodeIDMu.Unlock()
	_ = os.WriteFile(filepath.Join(storageDir, idFileName), []byte(id), 0600)
}

func loadSavedID() string {
	b, err := os.ReadFile(filepath.Join(storageDir, idFileName))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func blockPath(blockID string) string {
	return filepath.Join(storageDir, blockID)
}

func currentUsage() (int64, int) {
	entries, err := os.ReadDir(storageDir)
	if err != nil {
		return 0, 0
	}
	var used int64
	count := 0
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
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

func secretOK(got string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(clusterSecret)) == 1
}

// ----------------------------------------------------------------------------
// Cifrado en reposo (AES-256-GCM)
// ----------------------------------------------------------------------------

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

// writeAtomic escribe en un archivo temporal y luego lo renombra, para que
// una caida a mitad de escritura nunca deje un bloque truncado.
func writeAtomic(p string, data []byte) error {
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

// ----------------------------------------------------------------------------
// API de bloques (TCP + TLS)
// ----------------------------------------------------------------------------

func readFull(conn net.Conn, buf []byte) error {
	_, err := io.ReadFull(conn, buf)
	return err
}

func readShortString(conn net.Conn) (string, error) {
	lenBuf := make([]byte, 2)
	if err := readFull(conn, lenBuf); err != nil {
		return "", err
	}
	b := make([]byte, binary.BigEndian.Uint16(lenBuf))
	if err := readFull(conn, b); err != nil {
		return "", err
	}
	return string(b), nil
}

// readData lee "8 bytes de longitud + datos", rechazando tamanos absurdos
// (sin este tope, un paquete con longitud gigante tumbaba el nodo por memoria).
func readData(conn net.Conn) ([]byte, error) {
	lenBuf := make([]byte, 8)
	if err := readFull(conn, lenBuf); err != nil {
		return nil, err
	}
	dataLen := binary.BigEndian.Uint64(lenBuf)
	if dataLen > maxBlockBytes {
		return nil, fmt.Errorf("bloque demasiado grande (%d bytes, maximo %d)", dataLen, maxBlockBytes)
	}
	data := make([]byte, dataLen)
	if dataLen > 0 {
		if err := readFull(conn, data); err != nil {
			return nil, err
		}
	}
	return data, nil
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
	_ = conn.SetDeadline(time.Now().Add(2 * time.Minute))

	reqHeader := make([]byte, 5)
	if err := readFull(conn, reqHeader); err != nil {
		return
	}
	opcode := reqHeader[0]
	blockIDLen := binary.BigEndian.Uint32(reqHeader[1:])
	if blockIDLen == 0 || blockIDLen > maxBlockIDLen {
		_ = sendResponse(conn, statusError, []byte("block_id invalido"))
		return
	}
	blockIDBytes := make([]byte, blockIDLen)
	if err := readFull(conn, blockIDBytes); err != nil {
		return
	}
	blockID := string(blockIDBytes)
	if !validBlockID.MatchString(blockID) {
		_ = sendResponse(conn, statusError, []byte("block_id invalido"))
		return
	}

	// Las operaciones administrativas solo las puede pedir un nodo del cluster.
	if opcode == opDelete || opcode == opReplicate || opcode == opPutRaw {
		secret, err := readShortString(conn)
		if err != nil {
			return
		}
		if !secretOK(secret) {
			log.Printf("[DataNode %d] operacion %d rechazada: secreto invalido (%s)", dnPort, opcode, conn.RemoteAddr())
			_ = sendResponse(conn, statusError, []byte("no autorizado"))
			return
		}
	}

	switch opcode {
	case opPut:
		data, err := readData(conn)
		if err != nil {
			_ = sendResponse(conn, statusError, []byte(err.Error()))
			return
		}
		encrypted, err := encryptBlock(encryptionKey, data)
		if err != nil {
			_ = sendResponse(conn, statusError, []byte(err.Error()))
			return
		}
		if err := writeAtomic(blockPath(blockID), encrypted); err != nil {
			_ = sendResponse(conn, statusError, []byte(err.Error()))
			return
		}
		log.Printf("[DataNode %d] PUT %s (%d bytes, cifrado en reposo)", dnPort, blockID, len(data))
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
		targetHost, err := readShortString(conn)
		if err != nil {
			return
		}
		portBuf := make([]byte, 2)
		if err := readFull(conn, portBuf); err != nil {
			return
		}
		targetPort := int(binary.BigEndian.Uint16(portBuf))

		// Se copia el bloque tal cual esta en disco (ya cifrado): todos los
		// DataNodes comparten DN_ENCRYPTION_KEY, asi que el destino lo puede leer.
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
		data, err := readData(conn)
		if err != nil {
			_ = sendResponse(conn, statusError, []byte(err.Error()))
			return
		}
		if err := writeAtomic(blockPath(blockID), data); err != nil {
			_ = sendResponse(conn, statusError, []byte(err.Error()))
			return
		}
		log.Printf("[DataNode %d] PUT_RAW %s (%d bytes, replica P2P recibida)", dnPort, blockID, len(data))
		_ = sendResponse(conn, statusOK, []byte("ok"))

	default:
		_ = sendResponse(conn, statusError, []byte("opcode desconocido"))
	}
}

func serveBlocks(tlsCfg *tls.Config) {
	addr := fmt.Sprintf("%s:%d", dnHost, dnPort)
	ln, err := tls.Listen("tcp", addr, tlsCfg)
	if err != nil {
		log.Fatalf("[DataNode %d] no se pudo escuchar en %s: %v", dnPort, addr, err)
	}
	log.Printf("[DataNode %d] API de bloques (TLS) en %s (storage=%s)", dnPort, addr, storageDir)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[DataNode %d] error aceptando conexion: %v", dnPort, err)
			continue
		}
		go handleConn(conn)
	}
}

// putRawRemote conecta (TLS) a otro DataNode y le envia PUT_RAW con los
// bytes de un bloque. Es la mitad "DataNode <-> DataNode" de la re-replicacion.
func putRawRemote(host string, port int, blockID string, data []byte) error {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	cfg := &tls.Config{RootCAs: caPool, ServerName: host, MinVersion: tls.VersionTLS12}
	conn, err := tls.DialWithDialer(dialer, "tcp", fmt.Sprintf("%s:%d", host, port), cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Minute))

	var buf bytes.Buffer
	idBytes := []byte(blockID)
	buf.WriteByte(opPutRaw)
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(idBytes)))
	buf.Write(idBytes)
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(clusterSecret)))
	buf.WriteString(clusterSecret)
	_ = binary.Write(&buf, binary.BigEndian, uint64(len(data)))
	buf.Write(data)
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return err
	}

	respHeader := make([]byte, 5)
	if err := readFull(conn, respHeader); err != nil {
		return err
	}
	if respHeader[0] != statusOK {
		payloadLen := binary.BigEndian.Uint32(respHeader[1:])
		if payloadLen > 4096 {
			payloadLen = 4096
		}
		payload := make([]byte, payloadLen)
		_ = readFull(conn, payload)
		return fmt.Errorf("PUT_RAW fallo: %s", string(payload))
	}
	return nil
}

// ----------------------------------------------------------------------------
// API de administracion (HTTPS)
// ----------------------------------------------------------------------------

func handleHealth(w http.ResponseWriter, r *http.Request) {
	used, count := currentUsage()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"datanode_id": getID(), "host": dnPublicHost, "block_port": dnPort,
		"capacity": capacity, "used": used, "num_blocks": count,
	})
}

func serveAPI(tlsCfg *tls.Config) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	server := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", dnHost, dnAPIPort),
		Handler:           mux,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("[DataNode %d] API de administracion (HTTPS) en %s", dnPort, server.Addr)
	if err := server.ListenAndServeTLS("", ""); err != nil {
		log.Printf("[DataNode %d] API de administracion detenida: %v", dnPort, err)
	}
}

// ----------------------------------------------------------------------------
// Registro y heartbeat ante el ControlNode (REST/JSON sobre HTTPS)
// ----------------------------------------------------------------------------

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

func postToControlNode(url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(clusterSecretHeader, clusterSecret)
	return secureClient.Do(req)
}

// registerOnce intenta registrarse en cada ControlNode de la lista hasta
// encontrar al primario. Envia el ID anterior (si lo hay) para conservarlo.
func registerOnce() error {
	body, _ := json.Marshal(map[string]interface{}{
		"host": dnPublicHost, "port": dnPort, "capacity": capacity, "datanode_id": getID(),
	})
	var lastErr error
	for i := 0; i < len(controlNodeURLs); i++ {
		url := pickControlNodeURL()
		resp, err := postToControlNode(url+"/datanodes/register", body)
		if err != nil {
			lastErr = err
			markURLFailed()
			continue
		}
		if resp.StatusCode != 200 {
			respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			resp.Body.Close()
			lastErr = fmt.Errorf("%s respondio %d: %s", url, resp.StatusCode, strings.TrimSpace(string(respBody)))
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
		setID(out.DatanodeID)
		log.Printf("[DataNode %d] registrado ante ControlNode (%s) como %s", dnPort, url, out.DatanodeID)
		return nil
	}
	return lastErr
}

// registerWithRetry no se rinde: si el ControlNode aun no arranca (p. ej.
// en Docker), reintenta con espera creciente en vez de matar el proceso.
func registerWithRetry() {
	wait := 1 * time.Second
	for {
		err := registerOnce()
		if err == nil {
			return
		}
		log.Printf("[DataNode %d] registro fallido (%v); reintento en %s", dnPort, err, wait)
		time.Sleep(wait)
		if wait < 15*time.Second {
			wait *= 2
		}
	}
}

func heartbeatLoop() {
	for {
		time.Sleep(heartbeatInterval)
		used, count := currentUsage()
		body, _ := json.Marshal(map[string]interface{}{
			"datanode_id": getID(), "used": used, "num_blocks": count,
		})
		ok := false
		needRegister := false
		for i := 0; i < len(controlNodeURLs); i++ {
			url := pickControlNodeURL()
			resp, err := postToControlNode(url+"/datanodes/heartbeat", body)
			if err != nil {
				markURLFailed()
				continue
			}
			resp.Body.Close()
			switch resp.StatusCode {
			case 200:
				ok = true
			case 404:
				// El primario no nos conoce (p. ej. tras un failover antes de
				// que llegara la sincronizacion): hay que registrarse de nuevo.
				needRegister = true
			default:
				// 503 = ese ControlNode es standby; probar el siguiente.
				markURLFailed()
				continue
			}
			break
		}
		if needRegister {
			log.Printf("[DataNode %d] el ControlNode no me reconoce; re-registrando", dnPort)
			registerWithRetry()
			continue
		}
		if !ok {
			log.Printf("[DataNode %d] heartbeat fallo en todos los ControlNodes", dnPort)
		}
	}
}

// ----------------------------------------------------------------------------
// main
// ----------------------------------------------------------------------------

func main() {
	dnHost = getEnv("DN_HOST", "0.0.0.0")
	dnPort = getEnvInt("DN_PORT", 6001)
	dnAPIPort = getEnvInt("DN_API_PORT", dnPort+1000)
	dnPublicHost = getEnv("DN_PUBLIC_HOST", "127.0.0.1")
	storageDir = getEnv("DN_STORAGE_DIR", "./storage")
	capacity = getEnvInt64("DN_CAPACITY", 1*1024*1024*1024)
	maxBlockBytes = uint64(getEnvInt64("DN_MAX_BLOCK_BYTES", 128*1024*1024))
	heartbeatInterval = time.Duration(getEnvInt("DN_HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second
	for _, u := range strings.Split(getEnv("CONTROLNODE_URLS", getEnv("CONTROLNODE_URL", "https://127.0.0.1:5000,https://127.0.0.1:5001")), ",") {
		if u = strings.TrimSpace(u); u != "" {
			controlNodeURLs = append(controlNodeURLs, strings.TrimRight(u, "/"))
		}
	}
	certFile = getEnv("CERT_FILE", getEnv("CA_CERT_FILE", "../certs/cert.pem"))
	keyFile = getEnv("KEY_FILE", "../certs/key.pem")
	clusterSecret = getEnv("CLUSTER_SECRET", "dfsha-cluster-secret-CAMBIAR")
	encKey := getEnv("DN_ENCRYPTION_KEY", "")
	if encKey == "" {
		encKey = "clave-compartida-dfsha-2026"
		log.Printf("[DataNode %d] AVISO: DN_ENCRYPTION_KEY no definida; usando la clave de ejemplo", dnPort)
	}
	encryptionKey = deriveKey(encKey)

	if err := os.MkdirAll(storageDir, 0700); err != nil {
		log.Fatalf("[DataNode %d] no se pudo crear storage dir %s: %v", dnPort, storageDir, err)
	}
	datanodeID = loadSavedID()

	caPEM, err := os.ReadFile(certFile)
	if err != nil {
		log.Fatalf("[DataNode %d] no se pudo leer el certificado %s: %v", dnPort, certFile, err)
	}
	caPool = x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		log.Fatalf("[DataNode %d] %s no contiene un certificado PEM valido", dnPort, certFile)
	}
	serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("[DataNode %d] no se pudo cargar cert/key: %v", dnPort, err)
	}
	serverTLS := &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12}
	secureClient = &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: caPool, MinVersion: tls.VersionTLS12}},
	}

	// Primero se abren los puertos (para que el ControlNode ya pueda pedir
	// bloques) y luego se registra.
	go serveAPI(serverTLS)
	go func() {
		registerWithRetry()
		heartbeatLoop()
	}()
	serveBlocks(serverTLS)
}
