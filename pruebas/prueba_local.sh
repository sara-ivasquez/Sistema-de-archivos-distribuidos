#!/usr/bin/env bash
# Prueba end-to-end de DFSha en una sola maquina (sin Docker).
#
# Levanta 2 ControlNodes (primario/standby) y 3 DataNodes, y verifica:
#   1. register/login, mkdir, cd, put, ls, get (archivo identico)
#   2. aislamiento entre usuarios (bob no ve ni lee lo de alice)
#   3. no se puede borrar "/" ni sobrescribir un directorio con put
#   4. un cliente NO puede borrar bloques directamente en un DataNode
#   5. failover: cae el ControlNode primario, el standby atiende sin re-login
#   6. el primario viejo revive como STANDBY y no borra el estado
#   7. cae un DataNode: re-replicacion automatica y el get sigue funcionando
#
# Uso (desde la raiz del repo, con certs/ ya generado por gencert):
#   bash pruebas/prueba_local.sh
set -u
cd "$(dirname "$0")/.."
ROOT=$(pwd)
WORK=$(mktemp -d)
BIN="$WORK/bin"
mkdir -p "$BIN"
PIDS=()

ok()   { echo "  [OK]    $*"; }
fail() { echo "  [FALLA] $*"; FAILED=1; }
FAILED=0

cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done; wait 2>/dev/null; }
trap cleanup EXIT

echo "== Compilando (logs en $WORK)"
(cd controlnode_go && go build -o "$BIN/controlnode" .) || exit 1
(cd datanode_go && go build -o "$BIN/datanode" .) || exit 1

export CERT_FILE="$ROOT/certs/cert.pem" KEY_FILE="$ROOT/certs/key.pem"
export CLUSTER_SECRET="secreto-de-prueba" DN_ENCRYPTION_KEY="clave-de-prueba"
export DFSHA_CA_CERT="$ROOT/certs/cert.pem"
CN_ENV="BLOCK_SIZE=262144 REPLICATION_FACTOR=2 HEARTBEAT_TIMEOUT_SECONDS=6"

start_cn() { # nombre puerto rol puerto_peer
  env $CN_ENV CN_PORT=$2 CN_ROLE=$3 CN_PEER_URL=https://127.0.0.1:$4 \
      CN_STATE_FILE="$WORK/$1.json" "$BIN/controlnode" >>"$WORK/$1.log" 2>&1 &
  echo $!
}
start_dn() { # puerto
  DN_PORT=$1 DN_STORAGE_DIR="$WORK/storage$1" DN_HEARTBEAT_INTERVAL_SECONDS=2 \
  CONTROLNODE_URLS=https://127.0.0.1:5000,https://127.0.0.1:5001 \
      "$BIN/datanode" >>"$WORK/dn$1.log" 2>&1 &
  echo $!
}

echo "== Levantando 2 ControlNodes y 3 DataNodes"
CN1=$(start_cn cn1 5000 primary 5001); PIDS+=("$CN1")
CN2=$(start_cn cn2 5001 standby 5000); PIDS+=("$CN2")
sleep 2
DN1=$(start_dn 6001); PIDS+=("$DN1")
DN2=$(start_dn 6002); PIDS+=("$DN2")
DN3=$(start_dn 6003); PIDS+=("$DN3")
sleep 4

CLI="python3 $ROOT/client/cli.py"
cd "$WORK"
head -c 1500000 /dev/urandom > original.bin   # ~1.5 MB -> 6 bloques de 256 KB

echo "== 1. Operaciones basicas"
$CLI register alice clave1 >/dev/null; $CLI register bob clave2 >/dev/null
$CLI login alice clave1 | grep -q "Sesion iniciada" && ok "login alice" || fail "login alice"
$CLI mkdir /docs >/dev/null
$CLI cd /docs >/dev/null
[ "$($CLI pwd)" = "/docs" ] && ok "cd /docs" || fail "cd"
$CLI put original.bin informe.bin | tail -1 | grep -q "subido correctamente" && ok "put (rutas relativas)" || fail "put"
$CLI ls | grep -q "informe.bin" && ok "ls muestra el archivo" || fail "ls"
$CLI get informe.bin copia.bin >/dev/null && cmp -s original.bin copia.bin && ok "get: archivo identico" || fail "get"
echo "vacio" | head -c 0 > vacio.txt
$CLI put vacio.txt vacio.txt >/dev/null && $CLI ls | grep -q "vacio.txt.*0 bytes" && ok "archivo de 0 bytes en ls" || fail "archivo vacio"

echo "== 2. Aislamiento entre usuarios"
cp .dfsha_session.json alice.json
$CLI login bob clave2 >/dev/null
$CLI ls / | grep -q docs && fail "bob ve /docs de alice" || ok "bob no ve /docs de alice"
$CLI get /docs/informe.bin robado.bin | grep -q "permiso" && ok "bob no puede leer el archivo de alice" || fail "bob leyo"
$CLI rm /docs/informe.bin | grep -q "permiso" && ok "bob no puede borrar el archivo de alice" || fail "bob borro"
cp alice.json .dfsha_session.json

echo "== 3. Validaciones del arbol"
$CLI rmdir / | grep -q "raiz" && ok "rmdir / rechazado" || fail "rmdir /"
$CLI put original.bin /docs | grep -q "directorio" && ok "put sobre un directorio rechazado" || fail "put sobre dir"

echo "== 4. Un cliente no puede borrar bloques en un DataNode"
BLOCK=$(ls "$WORK/storage6001" | grep -v '^\.' | head -1)
python3 - "$BLOCK" <<'EOF' && ok "DELETE sin secreto rechazado" || fail "DELETE sin secreto aceptado"
import socket, ssl, struct, sys, os
ctx = ssl.create_default_context(cafile=os.environ["DFSHA_CA_CERT"])
s = ctx.wrap_socket(socket.create_connection(("127.0.0.1", 6001)), server_hostname="127.0.0.1")
bid = sys.argv[1].encode()
s.sendall(struct.pack("!BI", 3, len(bid)) + bid + struct.pack("!H", 3) + b"mal")
status = s.recv(1)[0]
sys.exit(0 if status == 0 else 1)
EOF
[ -f "$WORK/storage6001/$BLOCK" ] && ok "el bloque sigue en disco" || fail "el bloque fue borrado"

echo "== 5. Failover del ControlNode"
kill "$CN1"; wait "$CN1" 2>/dev/null
echo "   (primario detenido; esperando que el standby se promueva...)"
sleep 12
grep -q "asumiendo rol de PRIMARIO" "$WORK/cn2.log" && ok "standby promovido a primario" || fail "no hubo promocion"
$CLI ls /docs | grep -q informe.bin && ok "ls via el nuevo primario SIN volver a hacer login" || fail "ls tras failover"
$CLI get /docs/informe.bin copia2.bin >/dev/null && cmp -s original.bin copia2.bin && ok "get tras failover" || fail "get tras failover"
$CLI put original.bin /docs/nuevo.bin >/dev/null && ok "put tras failover (DataNodes re-registrados)" || fail "put tras failover"

echo "== 6. El primario viejo revive"
CN1=$(start_cn cn1 5000 primary 5001); PIDS+=("$CN1")
sleep 8
grep -q "arranco como STANDBY" "$WORK/cn1.log" && ok "revive como STANDBY (sin split-brain)" || fail "revivio como primario"
$CLI ls /docs | grep -q nuevo.bin && ok "el estado NO se borro" || fail "se perdio el estado"

echo "== 7. Caida de un DataNode"
kill "$DN1"; wait "$DN1" 2>/dev/null
echo "   (DataNode 6001 detenido; esperando timeout + re-replicacion...)"
sleep 22
grep -q "re-replicando bloque" "$WORK/cn2.log" && ok "re-replicacion automatica" || fail "no hubo re-replicacion"
$CLI get /docs/informe.bin copia3.bin >/dev/null && cmp -s original.bin copia3.bin && ok "get con un DataNode caido" || fail "get con DN caido"

echo
if [ "$FAILED" = 0 ]; then echo "TODAS LAS PRUEBAS PASARON"; else echo "HUBO FALLAS (logs en $WORK)"; fi
exit $FAILED
