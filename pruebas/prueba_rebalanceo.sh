#!/usr/bin/env bash
# Prueba del rebalanceo de bloques en una sola maquina:
#   15. se agrega un DataNode nuevo: recibe su parte de los bloques viejos
#       y los nodos llenos liberan espacio
#   16. un DataNode caido vuelve despues de la re-replicacion: las copias de
#       mas se recortan y cada bloque queda con REPLICATION_FACTOR copias
#   En ambos casos el archivo se sigue descargando identico.
#
# Uso (desde la raiz del repo, con certs/ ya generado por gencert):
#   bash pruebas/prueba_rebalanceo.sh
set -u
cd "$(dirname "$0")/.."
EXTRA_CN_ENV="GC_DELAY_SECONDS=5"
source pruebas/cluster.sh

bloques() { ls "$WORK/storage$1" 2>/dev/null | grep -v -e '^\.' -e '\.tmp$' | wc -l; }
conteo() { echo "dn6001=$(bloques 6001) dn6002=$(bloques 6002) dn6003=$(bloques 6003)"; }
# espera hasta 90 s a que se cumpla una condicion
esperar() { for _ in $(seq 1 45); do eval "$1" && return 0; sleep 2; done; return 1; }

echo "== Levantando 2 ControlNodes y SOLO 2 DataNodes"
CN1=$(start_cn cn1 5000 primary 5001); PIDS+=("$CN1")
CN2=$(start_cn cn2 5001 standby 5000); PIDS+=("$CN2")
sleep 2
DN1=$(start_dn 6001); PIDS+=("$DN1")
DN2=$(start_dn 6002); PIDS+=("$DN2")
sleep 4

CLI="python3 $ROOT/client/cli.py"
cd "$WORK"
head -c $((12 * 262144)) /dev/urandom > original.bin    # 12 bloques
$CLI register eva clave5 >/dev/null; $CLI login eva clave5 >/dev/null
$CLI put original.bin /grande.bin >/dev/null && ok "archivo de 12 bloques subido con 2 DataNodes ($(conteo))" || fail "put"

echo "== 15. Se agrega un DataNode nuevo"
DN3=$(start_dn 6003); PIDS+=("$DN3")
echo "   (esperando el rebalanceo...)"
equilibrado() { a=$(bloques 6001); b=$(bloques 6002); c=$(bloques 6003)
  max=$a; min=$a; for x in $b $c; do [ $x -gt $max ] && max=$x; [ $x -lt $min ] && min=$x; done
  [ $c -ge 6 ] && [ $((max - min)) -le 2 ] && [ $((a + b + c)) -eq 24 ]; }
esperar equilibrado && ok "bloques repartidos y copias viejas borradas ($(conteo))" || fail "no se equilibro ($(conteo))"
grep -q "rebalanceo:" "$WORK/cn1.log" && ok "el log del ControlNode registra los movimientos" || fail "sin log de rebalanceo"
$CLI get /grande.bin copia.bin >/dev/null && cmp -s original.bin copia.bin && ok "get tras el rebalanceo: archivo identico" || fail "get tras rebalanceo"

echo "== 16. Un DataNode cae y vuelve"
kill "$DN1"; wait "$DN1" 2>/dev/null
echo "   (DataNode 6001 detenido; esperando la re-replicacion...)"
rereplicado() { [ $(( $(bloques 6002) + $(bloques 6003) )) -eq 24 ]; }
esperar rereplicado && ok "sus bloques se re-replicaron en los otros 2 ($(conteo))" || fail "no se re-replico ($(conteo))"
DN1=$(start_dn 6001); PIDS+=("$DN1")
echo "   (DataNode 6001 de vuelta con sus bloques viejos; esperando el recorte...)"
grep -q "copias de mas" "$WORK/cn1.log" || esperar 'grep -q "copias de mas" "$WORK/cn1.log"'
grep -q "copias de mas" "$WORK/cn1.log" && ok "se detectaron y recortaron las copias de mas" || fail "no hubo recorte"
total24() { a=$(bloques 6001); b=$(bloques 6002); c=$(bloques 6003)
  max=$a; min=$a; for x in $b $c; do [ $x -gt $max ] && max=$x; [ $x -lt $min ] && min=$x; done
  [ $((a + b + c)) -eq 24 ] && [ $((max - min)) -le 2 ]; }
esperar total24 && ok "de nuevo 2 copias por bloque y carga pareja ($(conteo))" || fail "copias de mas en disco ($(conteo))"
$CLI get /grande.bin copia2.bin >/dev/null && cmp -s original.bin copia2.bin && ok "get final: archivo identico" || fail "get final"

echo
if [ "$FAILED" = 0 ]; then echo "TODAS LAS PRUEBAS DE REBALANCEO PASARON"; else echo "HUBO FALLAS (logs en $WORK)"; fi
exit $FAILED
