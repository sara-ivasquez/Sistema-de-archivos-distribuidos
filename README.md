# DFSha — Sistema de Archivos Distribuido de Alta Disponibilidad

Sistema de archivos distribuido (DFS) por bloques con arquitectura
Cliente/Servidor.

**Servicio distribuido (ControlNode + DataNode): Go**
**Cliente: Python**

## Requisitos previos

- [Go](https://go.dev/dl/) 1.21 o superior
- Python 3.10 o superior
- (Opcional) Docker y Docker Compose, para correr todo en contenedores

## Estructura del proyecto

```
controlnode_go/   ControlNode — servidor de metadatos (autenticación,
                  sistema de archivos, algoritmo de particionamiento y
                  colocación de bloques)
datanode_go/      DataNode — servidor de bloques (almacenamiento físico,
                  registro y heartbeat ante el ControlNode)
client/           Cliente CLI en Python (cli.py)
common/           Protocolo de transferencia de bloques (blockproto.py),
                  usado por el cliente para hablar con los DataNodes
proto/            Contrato formal del protocolo de bloques en protobuf
                  (block_service.proto), documentación de referencia
docker/           Dockerfiles y docker-compose.yml
requirements.txt  Dependencias de la parte Python (solo "requests")
```

## Componentes y responsabilidades

| Componente | Rol | Responsabilidad |
|---|---|---|
| **Cliente** | CLI | Autenticación, comandos de sistema de archivos (`ls`, `mkdir`, `rm`, `rmdir`), `put`/`get` de archivos |
| **ControlNode** | Maestro | Metadatos (estructura de directorios, ubicación de bloques), autenticación, algoritmo de particionamiento y colocación, registro/heartbeat de DataNodes |
| **DataNode** | Trabajador | Almacenamiento físico de bloques, atiende lectura/escritura directa desde el cliente |

## Cómo ejecutar (local, sin Docker)

**1. ControlNode** (una terminal, queda corriendo):
```bash
cd controlnode_go
go run main.go
```

**2. DataNodes** (una terminal por cada uno, quedan corriendo):
```bash
cd datanode_go
DN_PORT=6001 DN_PUBLIC_HOST=127.0.0.1 DN_STORAGE_DIR=./storage1 go run main.go
```
```bash
cd datanode_go
DN_PORT=6002 DN_PUBLIC_HOST=127.0.0.1 DN_STORAGE_DIR=./storage2 go run main.go
```
```bash
cd datanode_go
DN_PORT=6003 DN_PUBLIC_HOST=127.0.0.1 DN_STORAGE_DIR=./storage3 go run main.go
```

**3. Cliente** (una terminal, aquí se escriben los comandos):
```bash
pip install -r requirements.txt

python client/cli.py register alice 1234
python client/cli.py login alice 1234
python client/cli.py mkdir /docs
python client/cli.py put ./mi_archivo.pdf /docs/mi_archivo.pdf
python client/cli.py ls /docs
python client/cli.py get /docs/mi_archivo.pdf ./descargado.pdf
python client/cli.py rm /docs/mi_archivo.pdf
python client/cli.py rmdir /docs
```

**En Windows (PowerShell)**, las variables de entorno se definen así:
```powershell
$env:DN_PORT="6001"; $env:DN_PUBLIC_HOST="127.0.0.1"; $env:DN_STORAGE_DIR=".\storage1"
go run main.go
```

## Cómo ejecutar (con Docker Compose)

```bash
cd docker
docker compose up --build -d

mkdir -p ../client_files
cp /ruta/a/mi_archivo.pdf ../client_files/

docker compose run --rm client register alice 1234
docker compose run --rm client login alice 1234
docker compose run --rm client mkdir /docs
docker compose run --rm client put mi_archivo.pdf /docs/mi_archivo.pdf
docker compose run --rm client get /docs/mi_archivo.pdf descargado.pdf
```

## Comandos del cliente

| Comando | Descripción |
|---|---|
| `register <usuario> <password>` | Crea un usuario |
| `login <usuario> <password>` | Inicia sesión (guarda el token en `.dfsha_session.json`) |
| `mkdir <ruta>` | Crea un directorio |
| `ls [ruta]` | Lista el contenido de un directorio |
| `rmdir <ruta>` | Elimina un directorio vacío |
| `rm <ruta>` | Elimina un archivo (y sus bloques en los DataNodes) |
| `put <archivo_local> <ruta_remota>` | Sube un archivo (se particiona en bloques y se distribuye) |
| `get <ruta_remota> <archivo_local>` | Descarga y reconstruye un archivo |

## Variables de entorno

**ControlNode**: `CN_HOST` (0.0.0.0), `CN_PORT` (5000), `BLOCK_SIZE` (262144 bytes),
`REPLICATION_FACTOR` (2), `HEARTBEAT_TIMEOUT_SECONDS` (15)

**DataNode**: `DN_HOST` (0.0.0.0), `DN_PORT` (6001), `DN_PUBLIC_HOST` (127.0.0.1),
`DN_STORAGE_DIR` (./storage), `DN_CAPACITY` (1 GB), `CONTROLNODE_URL`
(http://127.0.0.1:5000), `DN_HEARTBEAT_INTERVAL_SECONDS` (5)

**Cliente**: `DFSHA_CONTROLNODE_URL` (http://127.0.0.1:5000), o el flag `--url`

## Algoritmo de particionamiento y colocación

Cada archivo se divide en bloques de tamaño fijo (`BLOCK_SIZE`). Para cada
bloque, el ControlNode elige `REPLICATION_FACTOR` DataNodes activos así:

1. Descarta los DataNodes sin heartbeat reciente (caídos).
2. Ordena los activos por carga relativa (espacio usado / capacidad), ascendente.
3. Rota el punto de inicio de esa lista según el índice del bloque, para no
   concentrar siempre los primeros bloques en los mismos nodos.
4. Toma los primeros N nodos distintos de esa lista rotada como réplicas.

## Protocolo de comunicación

- **Cliente ↔ ControlNode**: REST sobre HTTP, JSON.
- **Cliente ↔ DataNode**: protocolo binario propio sobre TCP (framing con
  longitud de campos), equivalente en mensajes y semántica al contrato
  gRPC definido en `proto/block_service.proto` (`PutBlock`, `GetBlock`,
  `DeleteBlock`, con streaming de bytes).

## Pendiente para el siguiente hito

- Alta disponibilidad del ControlNode (activo/standby).
- Consistencia 
- Rebalanceo automático de bloques al agregar/quitar DataNodes.
- Seguridad: TLS en las comunicaciones y cifrado en reposo de los bloques.
