#!/usr/bin/env bash
# Prueba de RF3 (acceso a archivos: open/read/write/lock/close) en una sola
# maquina. Levanta 2 ControlNodes y 3 DataNodes y ejecuta pruebas/rf3_casos.py:
#   8.  read parcial (solo trae los bloques del rango)
#   9.  write en medio, append y huecos (solo reescribe los bloques tocados)
#   10. locks: un escritor / varios lectores, rm y put respetan el lock
#   11. consistencia close-to-open (lectores ven versiones completas)
#   12. lectura concurrente desde 8 hilos
#   13. el lease libera el lock de un cliente que murio
#   14. failover del ControlNode con un archivo abierto
#
# Uso (desde la raiz del repo, con certs/ ya generado por gencert):
#   bash pruebas/prueba_rf3.sh
set -u
cd "$(dirname "$0")/.."
EXTRA_CN_ENV="HANDLE_TTL_SECONDS=6 GC_DELAY_SECONDS=20"
source pruebas/cluster.sh
start_cluster

CN1_PID=$CN1 BLOCK=262144 python3 pruebas/rf3_casos.py || FAILED=1
[ "$FAILED" = 0 ] || echo "(logs en $WORK)"
exit $FAILED
