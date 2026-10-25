// gencert genera el certificado TLS autofirmado del cluster DFSha.
//
// Todos los nodos (ControlNodes y DataNodes) usan el mismo par cert/key, y
// clientes y nodos confian SOLO en ese certificado (actua como su propia CA).
// Para que la verificacion TLS funcione, el certificado debe incluir todos
// los nombres/IPs con los que se contacta a los nodos.
//
// Uso:
//
//	cd gencert
//	go run main.go                                   # local + nombres de Docker
//	DFSHA_CERT_HOSTS="3.85.10.2,54.1.2.3" go run main.go   # agrega IPs de AWS
//
// Variables:
//
//	DFSHA_CERT_HOSTS  hosts extra (IPs o DNS) separados por coma
//	DFSHA_CERT_DIR    carpeta de salida (default ../certs)
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Nombres que siempre se incluyen: ejecucion local y servicios del
// docker-compose.yml.
var defaultHosts = []string{
	"localhost", "127.0.0.1", "::1",
	"controlnode1", "controlnode2", "datanode1", "datanode2", "datanode3",
}

func main() {
	outDir := os.Getenv("DFSHA_CERT_DIR")
	if outDir == "" {
		outDir = "../certs"
	}
	hosts := append([]string{}, defaultHosts...)
	for _, h := range strings.Split(os.Getenv("DFSHA_CERT_HOSTS"), ",") {
		if h = strings.TrimSpace(h); h != "" {
			hosts = append(hosts, h)
		}
	}

	var dnsNames []string
	var ips []net.IP
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			ips = append(ips, ip)
		} else {
			dnsNames = append(dnsNames, h)
		}
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatal(err)
	}
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		log.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               pkix.Name{CommonName: "DFSha cluster", Organization: []string{"DFSha"}},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              dnsNames,
		IPAddresses:           ips,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(filepath.Join(outDir, "cert.pem"), certPEM, 0644); err != nil {
		log.Fatal(err)
	}
	// 0644 para que los contenedores (otro usuario) la puedan leer desde el
	// volumen montado; la carpeta certs/ esta en .gitignore.
	if err := os.WriteFile(filepath.Join(outDir, "key.pem"), keyPEM, 0644); err != nil {
		log.Fatal(err)
	}
	log.Printf("Certificado generado en %s (cert.pem, key.pem) para: %s", outDir, strings.Join(hosts, ", "))
}
