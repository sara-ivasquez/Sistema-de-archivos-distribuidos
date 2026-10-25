"""
Casos de prueba de RF3 (acceso a archivos) usando el SDK client/dfsha.py.
Lo ejecuta pruebas/prueba_rf3.sh con el cluster ya levantado. Variables:
  CN1_PID          PID del ControlNode primario (para probar el failover)
  BLOCK            tamano de bloque configurado en el cluster
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from client import dfsha  # noqa: E402
from client.dfsha import DFSha, DFShaError, FileLockedError  # noqa: E402
from common import blockproto  # noqa: E402

URLS = ["https://127.0.0.1:5000", "https://127.0.0.1:5001"]
BS = int(os.environ.get("BLOCK", "262144"))
FAILED = False


def ok(msg):
    print(f"  [OK]    {msg}")


def fail(msg):
    global FAILED
    FAILED = True
    print(f"  [FALLA] {msg}")


def check(cond, msg):
    ok(msg) if cond else fail(msg)


def expect_locked(fn, msg):
    try:
        fn()
        fail(msg + " (NO se bloqueo)")
    except FileLockedError:
        ok(msg)


# Contadores de trafico hacia los DataNodes (para verificar que read/write
# solo tocan los bloques necesarios).
counts = {"get": 0, "put": 0}
_get, _put = blockproto.get_block, blockproto.put_block


def counting_get(*a, **k):
    counts["get"] += 1
    return _get(*a, **k)


def counting_put(*a, **k):
    counts["put"] += 1
    return _put(*a, **k)


dfsha.blockproto.get_block = counting_get
dfsha.blockproto.put_block = counting_put


def reset():
    counts["get"] = counts["put"] = 0


fs = DFSha(URLS)
try:
    fs.request("POST", "/auth/register", json={"username": "carla", "password": "clave3"})
except DFShaError:
    pass
fs.login("carla", "clave3")
try:
    fs.request("POST", "/fs/mkdir", json={"path": "/rf3"})
except DFShaError:
    pass
fs2 = DFSha(URLS, token=fs.token)  # "otro proceso" del mismo usuario

print("== 8. RF3: open / write / read parcial")
contenido = bytearray(os.urandom(int(BS * 2.5)))  # 3 bloques
with fs.open("/rf3/a.bin", "rw", create=True) as f:
    f.write(contenido)
with fs.open("/rf3/a.bin") as f:
    check(f.size == len(contenido), f"open de lectura: tamano {f.size}")
    reset()
    f.seek(BS - 10)
    check(f.read(20) == bytes(contenido[BS - 10:BS + 10]), "read que cruza el limite entre 2 bloques")
    check(counts["get"] == 2, f"ese read solo trajo 2 bloques de 3 (trajo {counts['get']})")
    reset()
    f.seek(5)
    check(f.read(100) == bytes(contenido[5:105]), "read de un rango pequeno")
    check(counts["get"] == 0, "un bloque ya leido sale de la cache del handle")

print("== 9. RF3: write parcial, append y huecos")
with fs.open("/rf3/a.bin", "rw") as f:
    v0 = f.version
    reset()
    f.seek(BS + 100)
    f.write(b"MODIFICADO")
    contenido[BS + 100:BS + 110] = b"MODIFICADO"
    f.flush()
    check(counts["put"] == 2, f"modificar 10 bytes reescribe 1 solo bloque x2 replicas ({counts['put']} PUT)")
    check(f.version == v0 + 1, "el commit sube la version del archivo")
    f.seek(0, 2)
    f.write(b"AL FINAL")
    contenido += b"AL FINAL"
with fs.open("/rf3/a.bin") as f:
    check(f.read() == bytes(contenido), "contenido correcto tras write en medio + append")

with fs.open("/rf3/hueco.bin", "rw", create=True) as f:
    f.write(b"inicio")
    f.seek(BS + 50)
    f.write(b"fin")
with fs.open("/rf3/hueco.bin") as f:
    data = f.read()
    check(len(data) == BS + 53 and data[:6] == b"inicio" and data[6:BS + 50] == b"\0" * (BS + 44)
          and data[-3:] == b"fin", "escribir mas alla del final rellena el hueco con ceros")

print("== 10. RF3: locks (un escritor, varios lectores)")
w = fs.open("/rf3/a.bin", "rw")
expect_locked(lambda: fs2.open("/rf3/a.bin", "rw"), "segundo escritor bloqueado")
r = fs2.open("/rf3/a.bin")
check(r.read(4) == bytes(contenido[:4]), "un lector sin lock puede leer mientras otro escribe")
expect_locked(lambda: r.lock("shared"), "lock compartido rechazado mientras hay un escritor")
expect_locked(lambda: fs2.request("DELETE", "/fs/rm", params={"path": "/rf3/a.bin"}), "rm bloqueado")
expect_locked(lambda: fs2.request("POST", "/files/put/init", json={"path": "/rf3/a.bin", "size": 1}),
              "put (send) sobre un archivo bloqueado rechazado")
w.close()
check(r.lock("shared") == "shared", "al cerrar el escritor, el lector toma un lock compartido")
r2 = fs2.open("/rf3/a.bin")
check(r2.lock("shared") == "shared", "dos lectores con lock compartido a la vez")
expect_locked(lambda: fs.open("/rf3/a.bin", "rw"), "nadie puede escribir mientras hay locks compartidos")
r.unlock()
r2.close()
w = fs.open("/rf3/a.bin", "rw")
ok("al soltar los locks compartidos, se puede escribir")
w.close()
r.close()

print("== 11. RF3: consistencia (cada lector ve una version completa)")
lector = fs2.open("/rf3/a.bin")
primer = lector.read(10)
with fs.open("/rf3/a.bin", "rw") as f:
    for i in range(3):
        f.seek(i * BS)
        f.write(b"V2" * 50)
lector.seek(BS)
check(lector.read(10) == bytes(contenido[BS:BS + 10]),
      "un lector abierto antes del write sigue viendo la version vieja completa")
lector.close()
with fs.open("/rf3/a.bin") as f:
    f.seek(BS)
    check(f.read(4) == b"V2V2", "al reabrir se ve la version nueva (close-to-open)")
    check(primer == bytes(contenido[:10]), "lo leido antes del write corresponde a la version vieja")

print("== 12. RF3: lectura concurrente (8 hilos sobre el mismo archivo)")
with fs.open("/rf3/a.bin") as f:
    esperado = f.read()
errores = []


def lector_hilo(k):
    c = DFSha(URLS, token=fs.token)
    with c.open("/rf3/a.bin") as h:
        h.lock("shared")
        off = (k * 37771) % (len(esperado) - 5000)
        h.seek(off)
        if h.read(5000) != esperado[off:off + 5000]:
            errores.append(k)


hilos = [threading.Thread(target=lector_hilo, args=(k,)) for k in range(8)]
t0 = time.time()
[h.start() for h in hilos]
[h.join() for h in hilos]
check(not errores, f"8 lectores concurrentes con lock compartido OK ({time.time() - t0:.2f} s)")

print("== 13. RF3: el lease libera el lock de un cliente que murio")
muerto = DFSha(URLS, token=fs.token)
fm = muerto.open("/rf3/a.bin", "rw")
muerto._renew_stop.set()  # simula que el proceso murio: nadie renueva ni cierra
expect_locked(lambda: fs.open("/rf3/a.bin", "rw"), "mientras el lease esta vigente, sigue bloqueado")
liberado = False
for _ in range(30):
    time.sleep(1)
    try:
        fs.open("/rf3/a.bin", "rw").close()
        liberado = True
        break
    except FileLockedError:
        pass
check(liberado, "al expirar el lease, el lock se libera solo")

print("== 14. RF3: failover del ControlNode con un archivo abierto")
f = fs.open("/rf3/failover.txt", "rw", create=True)
f.write(b"antes de la caida|")
f.flush()
os.kill(int(os.environ["CN1_PID"]), 15)
print("   (primario detenido con el archivo abierto; esperando la promocion...)")
time.sleep(12)
f.write(b"despues de la caida")
f.close()
check(fs.active_index == 1, "el SDK paso solo al ControlNode standby")
with fs.open("/rf3/failover.txt") as g:
    check(g.read() == b"antes de la caida|despues de la caida",
          "el mismo handle siguio funcionando y el contenido es correcto")

print()
print("TODAS LAS PRUEBAS RF3 PASARON" if not FAILED else "HUBO FALLAS EN RF3")
sys.exit(1 if FAILED else 0)
