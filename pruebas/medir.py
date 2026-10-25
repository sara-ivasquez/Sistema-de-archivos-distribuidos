"""
Mediciones de rendimiento de DFSha (para la seccion "Pruebas y analisis de
resultados" del informe). Corre en Windows, Linux o macOS: solo necesita
Python y "requests", como el cliente.

Uso (con las mismas variables que el cliente):

    # Windows PowerShell
    $env:DFSHA_CONTROLNODE_URL="https://IP_CN1:5000,https://IP_CN2:5000"
    $env:DFSHA_CA_CERT="certs\\cert.pem"
    python pruebas\\medir.py              # bateria completa (~15-20 min)
    python pruebas\\medir.py --rapido     # version corta (~3-5 min)
    python pruebas\\medir.py failover     # mide la caida del ControlNode

Que mide (bateria completa):
  1. Latencia de operaciones de metadatos (login, ls, mkdir) contra el ControlNode.
  2. put/get de archivos de varios tamanos con 1 y 4 transferencias en paralelo.
  3. Efecto del paralelismo (1, 2, 4 y 8 hilos) sobre un archivo fijo.
  4. Lectura parcial (RF3): leer 64 bytes de la mitad de un archivo grande.
  5. Lectores concurrentes: N clientes descargando el mismo archivo a la vez.

Modo "failover": consulta el ControlNode cada 0,5 s. Mientras corre, apaga
el ControlNode primario en su VM; el script mide cuantos segundos estuvo
el servicio sin responder hasta que el standby tomo el control.

Resultados: se imprimen en pantalla y se guardan en pruebas/resultados/
como .csv (datos crudos) y .md (tablas listas para el informe).
"""
import argparse
import csv
import hashlib
import os
import statistics
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from client.dfsha import DFSha, DFShaError  # noqa: E402
from common import blockproto  # noqa: E402

MB = 1024 * 1024
USER, PASSWORD = "medicion", "medicion-dfsha"
DIR = "/mediciones"


# ----------------------------------------------------------------------------
# put / get instrumentados (misma logica que client/cli.py, sin imprimir)
# ----------------------------------------------------------------------------

def put_file(fs, local, remote, workers):
    size = os.path.getsize(local)
    t0 = time.perf_counter()
    info = fs.request("POST", "/files/put/init", json={"path": remote, "size": size})
    bs, quorum = info["block_size"], info["write_quorum"]

    def upload(b):
        with open(local, "rb") as f:
            f.seek(b["index"] * bs)
            chunk = f.read(bs)
        ok = 0
        for rep in b["replicas"]:
            try:
                blockproto.put_block(rep["host"], rep["port"], b["block_id"], chunk)
                ok += 1
            except Exception:  # noqa: BLE001
                pass
        if ok < quorum:
            raise RuntimeError(f"bloque {b['index']}: quorum no alcanzado")
        return b["block_id"], hashlib.sha256(chunk).hexdigest()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        checksums = dict(pool.map(upload, info["blocks"]))
    fs.request("POST", "/files/put/complete", json={"path": remote, "checksums": checksums})
    return time.perf_counter() - t0, len(info["blocks"])


def get_file(fs, remote, out, workers):
    t0 = time.perf_counter()
    info = fs.request("GET", "/files/get", params={"path": remote})
    bs = info["block_size"]

    def download(b):
        for rep in b["replicas"]:
            try:
                data = blockproto.get_block(rep["host"], rep["port"], b["block_id"])
                if b.get("checksum") and hashlib.sha256(data).hexdigest() != b["checksum"]:
                    continue
                return b["index"], data
            except Exception:  # noqa: BLE001
                continue
        raise RuntimeError(f"bloque {b['index']}: todas las replicas fallaron")

    lock = threading.Lock()
    with open(out, "wb") as f, ThreadPoolExecutor(max_workers=workers) as pool:
        for idx, data in pool.map(download, info["blocks"]):
            with lock:
                f.seek(idx * bs)
                f.write(data)
    return time.perf_counter() - t0


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(MB), b""):
            h.update(chunk)
    return h.hexdigest()


def make_file(folder, size_mb):
    p = os.path.join(folder, f"medicion_{size_mb}MB.bin")
    if not os.path.exists(p) or os.path.getsize(p) != size_mb * MB:
        with open(p, "wb") as f:
            for _ in range(size_mb):
                f.write(os.urandom(MB))
    return p


# ----------------------------------------------------------------------------
# Bateria de mediciones
# ----------------------------------------------------------------------------

class Results:
    def __init__(self):
        self.rows = []

    def add(self, prueba, **kw):
        row = {"prueba": prueba, **kw}
        self.rows.append(row)
        detalle = ", ".join(f"{k}={v}" for k, v in kw.items())
        print(f"  [{prueba}] {detalle}", flush=True)


def connect(urls):
    fs = DFSha(urls)
    try:
        fs.request("POST", "/auth/register", json={"username": USER, "password": PASSWORD})
    except DFShaError:
        pass  # ya existe
    fs.login(USER, PASSWORD)
    try:
        fs.request("POST", "/fs/mkdir", json={"path": DIR})
    except DFShaError:
        pass
    return fs


def medir_metadatos(fs, res, n):
    print("\n== 1. Latencia de metadatos (ControlNode)")
    for nombre, fn in [
        ("login", lambda: fs.login(USER, PASSWORD)),
        ("ls", lambda: fs.request("GET", "/fs/ls", params={"path": DIR})),
        ("datanodes/status", lambda: fs.request("GET", "/datanodes/status")),
    ]:
        tiempos = []
        for _ in range(n):
            t0 = time.perf_counter()
            fn()
            tiempos.append((time.perf_counter() - t0) * 1000)
        res.add("metadatos", operacion=nombre, repeticiones=n,
                promedio_ms=round(statistics.mean(tiempos), 1),
                mediana_ms=round(statistics.median(tiempos), 1),
                min_ms=round(min(tiempos), 1), max_ms=round(max(tiempos), 1))


def medir_transferencias(fs, res, folder, sizes, workers_list, reps):
    print("\n== 2. put/get por tamano de archivo y paralelismo")
    for size_mb in sizes:
        local = make_file(folder, size_mb)
        original = sha256_file(local)
        for w in workers_list:
            for rep in range(1, reps + 1):
                remote = f"{DIR}/archivo_{size_mb}MB.bin"
                t_put, nblocks = put_file(fs, local, remote, w)
                out = os.path.join(folder, "descarga.bin")
                t_get = get_file(fs, remote, out, w)
                ok = sha256_file(out) == original
                res.add("transferencia", tamano_mb=size_mb, bloques=nblocks, hilos=w, repeticion=rep,
                        put_s=round(t_put, 2), put_mb_s=round(size_mb / t_put, 2),
                        get_s=round(t_get, 2), get_mb_s=round(size_mb / t_get, 2),
                        integridad="OK" if ok else "FALLO")
                fs.request("DELETE", "/fs/rm", params={"path": remote})


def medir_paralelismo(fs, res, folder, size_mb, workers_list):
    print(f"\n== 3. Efecto del paralelismo (archivo de {size_mb} MB)")
    local = make_file(folder, size_mb)
    remote = f"{DIR}/paralelismo.bin"
    for w in workers_list:
        t_put, nblocks = put_file(fs, local, remote, w)
        t_get = get_file(fs, remote, os.path.join(folder, "descarga.bin"), w)
        res.add("paralelismo", tamano_mb=size_mb, bloques=nblocks, hilos=w,
                put_s=round(t_put, 2), put_mb_s=round(size_mb / t_put, 2),
                get_s=round(t_get, 2), get_mb_s=round(size_mb / t_get, 2))
    fs.request("DELETE", "/fs/rm", params={"path": remote})


def medir_lectura_parcial(fs, res, folder, size_mb, n):
    print(f"\n== 4. Lectura parcial RF3 (64 bytes de un archivo de {size_mb} MB)")
    local = make_file(folder, size_mb)
    remote = f"{DIR}/parcial.bin"
    put_file(fs, local, remote, 4)
    with open(local, "rb") as f:
        f.seek(size_mb * MB // 2)
        esperado = f.read(64)
    tiempos, correctos = [], 0
    for _ in range(n):
        t0 = time.perf_counter()
        with fs.open(remote) as h:  # handle nuevo cada vez: sin cache
            h.seek(size_mb * MB // 2)
            data = h.read(64)
        tiempos.append(time.perf_counter() - t0)
        correctos += data == esperado
    t_full = get_file(fs, remote, os.path.join(folder, "descarga.bin"), 4)
    res.add("lectura_parcial", tamano_mb=size_mb, bytes_leidos=64, repeticiones=n,
            promedio_s=round(statistics.mean(tiempos), 3), get_completo_s=round(t_full, 2),
            aceleracion=f"{t_full / statistics.mean(tiempos):.0f}x", correctas=f"{correctos}/{n}")
    fs.request("DELETE", "/fs/rm", params={"path": remote})


def medir_concurrencia(fs, res, folder, size_mb, clientes_list):
    print(f"\n== 5. Lectores concurrentes (archivo de {size_mb} MB)")
    local = make_file(folder, size_mb)
    remote = f"{DIR}/concurrente.bin"
    put_file(fs, local, remote, 4)
    original = sha256_file(local)
    for n in clientes_list:
        errores = []

        def cliente(k):
            c = DFSha(fs.urls, token=fs.token, ca_cert=fs.ca_cert)
            out = os.path.join(folder, f"concurrente_{k}.bin")
            t = get_file(c, remote, out, 2)
            if sha256_file(out) != original:
                errores.append(k)
            os.remove(out)
            return t

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=n) as pool:
            tiempos = list(pool.map(cliente, range(n)))
        total = time.perf_counter() - t0
        res.add("concurrencia", tamano_mb=size_mb, clientes=n, tiempo_total_s=round(total, 2),
                tiempo_promedio_cliente_s=round(statistics.mean(tiempos), 2),
                throughput_agregado_mb_s=round(size_mb * n / total, 2),
                integridad="OK" if not errores else f"FALLO {errores}")
    fs.request("DELETE", "/fs/rm", params={"path": remote})


# ----------------------------------------------------------------------------
# Modo failover
# ----------------------------------------------------------------------------

def medir_failover(urls, res):
    fs = connect(urls)
    print("\n== Failover del ControlNode")
    print("Consultando el ControlNode cada 0,5 s.")
    print(">>> AHORA apaga el ControlNode primario en su VM:")
    print("    sudo docker compose -f docker/aws/controlnode.compose.yml stop")
    print("(Ctrl+C para cancelar; el script termina solo cuando el servicio se recupera)\n")
    caida, inicial = None, fs.active_index
    while True:
        t = time.perf_counter()
        try:
            fs.request("GET", "/fs/ls", params={"path": "/"})
            if caida is not None:
                down = t - caida
                print(f"\n  Servicio recuperado en {fs.urls[fs.active_index]} "
                      f"tras {down:.1f} s sin respuesta.")
                res.add("failover", controlnode_inicial=fs.urls[inicial],
                        controlnode_final=fs.urls[fs.active_index],
                        segundos_sin_servicio=round(down, 1), relogin="no requerido")
                return
            print(".", end="", flush=True)
        except DFShaError:
            if caida is None:
                caida = t
                print("\n  ControlNode primario caido; esperando al standby", end="", flush=True)
            else:
                print("x", end="", flush=True)
        time.sleep(0.5)


# ----------------------------------------------------------------------------
# Salida
# ----------------------------------------------------------------------------

def guardar(res, args):
    os.makedirs(os.path.join(os.path.dirname(__file__), "resultados"), exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.join(os.path.dirname(__file__), "resultados", f"mediciones_{stamp}")
    keys = []
    for r in res.rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with open(base + ".csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(res.rows)

    with open(base + ".md", "w", encoding="utf-8") as f:
        f.write(f"# Mediciones DFSha — {datetime.now():%Y-%m-%d %H:%M}\n\n")
        f.write(f"- ControlNodes: {', '.join(args.urls)}\n")
        f.write(f"- Modo: {args.modo}{' (rapido)' if args.rapido else ''}\n\n")
        for prueba in dict.fromkeys(r["prueba"] for r in res.rows):
            filas = [r for r in res.rows if r["prueba"] == prueba]
            cols = [k for k in filas[0] if k != "prueba"]
            f.write(f"## {prueba}\n\n| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n")
            for r in filas:
                f.write("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |\n")
            f.write("\n")
    print(f"\nResultados guardados en:\n  {base}.md\n  {base}.csv")
    return base


def main():
    ap = argparse.ArgumentParser(description="Mediciones de rendimiento de DFSha")
    ap.add_argument("modo", nargs="?", default="todo", choices=["todo", "failover"])
    ap.add_argument("--rapido", action="store_true", help="bateria corta (~3-5 min)")
    ap.add_argument("--url", default=os.environ.get("DFSHA_CONTROLNODE_URL", "https://127.0.0.1:5000,https://127.0.0.1:5001"))
    args = ap.parse_args()
    args.urls = [u.strip() for u in args.url.split(",") if u.strip()]

    res = Results()
    t0 = time.time()
    try:
        if args.modo == "failover":
            medir_failover(args.urls, res)
        else:
            fs = connect(args.urls)
            with tempfile.TemporaryDirectory(prefix="dfsha_medir_") as folder:
                if args.rapido:
                    medir_metadatos(fs, res, 10)
                    medir_transferencias(fs, res, folder, [1, 10], [1, 4], 1)
                    medir_paralelismo(fs, res, folder, 20, [1, 4])
                    medir_lectura_parcial(fs, res, folder, 20, 3)
                    medir_concurrencia(fs, res, folder, 10, [1, 4])
                else:
                    medir_metadatos(fs, res, 20)
                    medir_transferencias(fs, res, folder, [1, 10, 50, 100], [1, 4], 1)
                    medir_paralelismo(fs, res, folder, 50, [1, 2, 4, 8])
                    medir_lectura_parcial(fs, res, folder, 100, 5)
                    medir_concurrencia(fs, res, folder, 20, [1, 2, 4])
    except KeyboardInterrupt:
        print("\nInterrumpido; se guardan los resultados obtenidos hasta ahora.")
    except DFShaError as exc:
        print(f"\nError del sistema: {exc}")
    finally:
        if res.rows:
            guardar(res, args)
            print(f"Duracion total: {(time.time() - t0) / 60:.1f} min")


if __name__ == "__main__":
    main()
