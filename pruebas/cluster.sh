# Funciones comunes para levantar un cluster DFSha local en las pruebas.
# Se usa con "source pruebas/cluster.sh" desde la raiz del repo.
ROOT=$(pwd)
WORK=$(mktemp -d)
BIN="$WORK/bin"
mkdir -p "$BIN"
PIDS=()
FAILED=0

ok()   { echo "  [OK]    $*"; }
fail() { echo "  [FALLA] $*"; FAILED=1; }

cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done; wait 2>/dev/null; }
trap cleanup EXIT

echo "== Compilando (logs en $WORK)"
(cd controlnode_go && go build -o "$BIN/controlnode" .) || exit 1
(cd datanode_go && go build -o "$BIN/datanode" .) || exit 1

export CERT_FILE="$ROOT/certs/cert.pem" KEY_FILE="$ROOT/certs/key.pem"
export CLUSTER_SECRET="secreto-de-prueba" DN_ENCRYPTION_KEY="clave-de-prueba"
export DFSHA_CA_CERT="$ROOT/certs/cert.pem"
CN_ENV="BLOCK_SIZE=262144 REPLICATION_FACTOR=2 HEARTBEAT_TIMEOUT_SECONDS=6 ${EXTRA_CN_ENV:-}"

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

start_cluster() {
  echo "== Levantando 2 ControlNodes y 3 DataNodes"
  CN1=$(start_cn cn1 5000 primary 5001); PIDS+=("$CN1")
  CN2=$(start_cn cn2 5001 standby 5000); PIDS+=("$CN2")
  sleep 2
  DN1=$(start_dn 6001); PIDS+=("$DN1")
  DN2=$(start_dn 6002); PIDS+=("$DN2")
  DN3=$(start_dn 6003); PIDS+=("$DN3")
  sleep 4
}
