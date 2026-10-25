"""
Cliente DFSha (RF1, RF2, RF4).

Uso:
    python3 client/cli.py register alice 1234
    python3 client/cli.py login alice 1234
    python3 client/cli.py mkdir /docs
    python3 client/cli.py cd /docs
    python3 client/cli.py pwd
    python3 client/cli.py ls
    python3 client/cli.py put ./informe.pdf informe.pdf
    python3 client/cli.py get informe.pdf ./descargado.pdf
    python3 client/cli.py rm informe.pdf
    python3 client/cli.py cd /
    python3 client/cli.py rmdir /docs

Acceso a archivos (RF3, ver client/dfsha.py):
    python3 client/cli.py cat notas.txt --offset 100 --length 50
    python3 client/cli.py write notas.txt 0 "texto que sobrescribe desde el byte 0"
    python3 client/cli.py write notas.txt end "texto que se agrega al final"
    python3 client/cli.py shell      # open/read/write/seek/lock/close interactivos

ControlNodes: --url o DFSHA_CONTROLNODE_URL, lista separada por comas
(primario y standby). Si uno no responde o esta en standby (503), el
cliente prueba el siguiente automaticamente (failover transparente).

TLS: el cliente solo confia en el certificado del cluster (certs/cert.pem,
o la ruta en DFSHA_CA_CERT); nunca desactiva la verificacion.

La sesion (token, ControlNode activo y directorio actual) se guarda en
.dfsha_session.json en el directorio desde el que se ejecuta.
"""
import argparse
import hashlib
import json
import os
import posixpath
import shlex
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import blockproto  # noqa: E402
from client.dfsha import DFSha, DFShaError  # noqa: E402

SESSION_FILE = ".dfsha_session.json"
DEFAULT_URLS = "https://127.0.0.1:5000,https://127.0.0.1:5001"
CA_CERT = os.environ.get("DFSHA_CA_CERT", blockproto.DEFAULT_CA)
WORKERS = int(os.environ.get("DFSHA_WORKERS", "4"))


# ----------------------------------------------------------------------------
# Sesion y conexion con el ControlNode
# ----------------------------------------------------------------------------

def load_session():
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE) as f:
            return json.load(f)
    return {}


def save_session(data):
    with open(SESSION_FILE, "w") as f:
        json.dump(data, f)


def resolve_urls(raw):
    return [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]


def try_urls(urls, start_index, method, path, **kwargs):
    """Intenta cada ControlNode empezando por start_index. Salta los que no
    responden y los que estan en standby (503). Devuelve (respuesta, indice)."""
    last_err = None
    n = len(urls)
    for offset in range(n):
        i = (start_index + offset) % n
        try:
            r = requests.request(method, f"{urls[i]}{path}", timeout=10, verify=CA_CERT, **kwargs)
        except requests.exceptions.SSLError as exc:
            raise SystemExit(f"Error TLS con {urls[i]}: {exc}\n"
                             f"Verifica que {CA_CERT} sea el certificado del cluster y que incluya ese host.")
        except requests.exceptions.RequestException as exc:
            last_err = exc
            continue
        if r.status_code == 503:
            last_err = f"{urls[i]} esta en standby"
            continue
        return r, i
    raise SystemExit(f"Ningun ControlNode disponible en {urls}: {last_err}")


def session_request(session, method, path, **kwargs):
    urls = session.get("urls") or resolve_urls(DEFAULT_URLS)
    idx = session.get("active_index", 0)
    r, new_idx = try_urls(urls, idx, method, path, **kwargs)
    if new_idx != idx:
        print(f"(failover: ahora se usa el ControlNode {urls[new_idx]})")
        session["active_index"] = new_idx
        save_session(session)
    if r.status_code == 401:
        raise SystemExit("La sesion expiro o no es valida. Vuelve a hacer login.")
    return r


def auth_headers(session):
    token = session.get("token")
    if not token:
        raise SystemExit("No has iniciado sesion. Usa: cli.py login <usuario> <password>")
    return {"Authorization": f"Bearer {token}"}


def error_of(r):
    try:
        return r.json().get("error", r.text)
    except ValueError:
        return r.text


def resolve_path(session, p):
    """Convierte una ruta relativa en absoluta segun el directorio actual (cd)."""
    cwd = session.get("cwd", "/")
    return posixpath.normpath(posixpath.join(cwd, p or "."))


# ----------------------------------------------------------------------------
# Comandos
# ----------------------------------------------------------------------------

def cmd_register(args, session):
    urls = resolve_urls(args.url)
    r, _ = try_urls(urls, 0, "POST", "/auth/register", json={"username": args.username, "password": args.password})
    print(f"Usuario '{args.username}' creado." if r.ok else f"Error: {error_of(r)}")


def cmd_login(args, session):
    urls = resolve_urls(args.url)
    r, idx = try_urls(urls, 0, "POST", "/auth/login", json={"username": args.username, "password": args.password})
    if r.ok:
        save_session({"urls": urls, "active_index": idx, "token": r.json()["token"],
                      "username": args.username, "cwd": "/"})
        print(f"Sesion iniciada como {args.username} (ControlNode activo: {urls[idx]}).")
    else:
        print("Error:", error_of(r))


def cmd_pwd(args, session):
    print(session.get("cwd", "/"))


def cmd_cd(args, session):
    target = resolve_path(session, args.path)
    r = session_request(session, "GET", "/fs/ls", params={"path": target}, headers=auth_headers(session))
    if not r.ok:
        print("Error:", error_of(r))
        return
    session["cwd"] = target
    save_session(session)
    print(target)


def cmd_mkdir(args, session):
    r = session_request(session, "POST", "/fs/mkdir", json={"path": resolve_path(session, args.path)},
                        headers=auth_headers(session))
    print("OK" if r.ok else f"Error: {error_of(r)}")


def cmd_rmdir(args, session):
    target = resolve_path(session, args.path)
    r = session_request(session, "DELETE", "/fs/rmdir", params={"path": target}, headers=auth_headers(session))
    if r.ok and posixpath.commonpath([session.get("cwd", "/"), target]) == target:
        session["cwd"] = posixpath.dirname(target) or "/"  # borramos el dir actual: subir uno
        save_session(session)
    print("OK" if r.ok else f"Error: {error_of(r)}")


def cmd_ls(args, session):
    target = resolve_path(session, args.path)
    r = session_request(session, "GET", "/fs/ls", params={"path": target}, headers=auth_headers(session))
    if not r.ok:
        print("Error:", error_of(r))
        return
    entries = r.json()["entries"]
    if not entries:
        print("  (vacio)")
    for e in entries:
        if e["type"] == "dir":
            print(f"  [dir]  {e['name']}")
        else:
            print(f"  [file] {e['name']}\t{e.get('size', 0)} bytes\t{e.get('status', '')}")


def cmd_rm(args, session):
    r = session_request(session, "DELETE", "/fs/rm", params={"path": resolve_path(session, args.path)},
                        headers=auth_headers(session))
    print("OK" if r.ok else f"Error: {error_of(r)}")


def cmd_put(args, session):
    remote = resolve_path(session, args.remote_path)
    size = os.path.getsize(args.local_path)

    r = session_request(session, "POST", "/files/put/init", json={"path": remote, "size": size},
                        headers=auth_headers(session))
    if not r.ok:
        print("Error:", error_of(r))
        return
    info = r.json()
    block_size = info["block_size"]
    blocks = info["blocks"]
    write_quorum = info.get("write_quorum", len(blocks[0]["replicas"]) if blocks else 1)
    print(f"Subiendo '{args.local_path}' ({size} bytes) a {remote} en {len(blocks)} bloque(s), "
          f"replicacion x{len(blocks[0]['replicas']) if blocks else 0}, quorum={write_quorum}, "
          f"{WORKERS} en paralelo...")

    file_lock = threading.Lock()
    start = time.time()

    def upload(b):
        # El cliente escribe directo en cada DataNode (el ControlNode no toca los datos).
        with file_lock, open(args.local_path, "rb") as f:
            f.seek(b["index"] * block_size)
            chunk = f.read(block_size)
        successes, failures = 0, []
        for replica in b["replicas"]:
            try:
                blockproto.put_block(replica["host"], replica["port"], b["block_id"], chunk)
                successes += 1
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{replica['host']}:{replica['port']} -> {exc}")
        return b, hashlib.sha256(chunk).hexdigest(), successes, failures

    checksums, failed = {}, False
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for b, checksum, successes, failures in pool.map(upload, blocks):
            checksums[b["block_id"]] = checksum
            ok = successes >= write_quorum
            print(f"  bloque {b['index']}: {successes}/{len(b['replicas'])} replicas "
                  f"[{'OK' if ok else 'QUORUM NO ALCANZADO'}]")
            if not ok:
                failed = True
                print("    Fallas:", "; ".join(failures) if failures else "(ninguna replica)")
    if failed:
        print("Subida ABORTADA: algun bloque no alcanzo el quorum de escritura.")
        sys.exit(1)

    elapsed = time.time() - start
    r = session_request(session, "POST", "/files/put/complete", json={"path": remote, "checksums": checksums},
                        headers=auth_headers(session))
    if r.ok:
        print(f"Archivo subido correctamente en {elapsed:.2f} s "
              f"({size / max(elapsed, 1e-6) / 1024 / 1024:.2f} MB/s).")
    else:
        print(f"Error al finalizar: {error_of(r)}")


def cmd_get(args, session):
    remote = resolve_path(session, args.remote_path)
    r = session_request(session, "GET", "/files/get", params={"path": remote}, headers=auth_headers(session))
    if not r.ok:
        print("Error:", error_of(r))
        return
    info = r.json()
    blocks = sorted(info["blocks"], key=lambda b: b["index"])
    block_size = info["block_size"]
    print(f"Descargando '{remote}' ({info['size']} bytes) en {len(blocks)} bloque(s), {WORKERS} en paralelo...")

    def download(b):
        expected = b.get("checksum")
        errors = []
        for replica in b["replicas"]:
            try:
                data = blockproto.get_block(replica["host"], replica["port"], b["block_id"])
                if expected and hashlib.sha256(data).hexdigest() != expected:
                    raise ValueError("checksum no coincide (bloque corrupto)")
                return b, data, replica, errors
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{replica['host']}:{replica['port']} ({exc})")
        return b, None, None, errors

    start = time.time()
    file_lock = threading.Lock()
    failed = False
    with open(args.local_path, "wb") as out, ThreadPoolExecutor(max_workers=WORKERS) as pool:
        out.truncate(info["size"])
        for b, data, replica, errors in pool.map(download, blocks):
            for e in errors:
                print(f"  bloque {b['index']}: replica {e} fallo, se probo la siguiente")
            if data is None:
                print(f"  bloque {b['index']}: TODAS las replicas fallaron")
                failed = True
                continue
            with file_lock:
                out.seek(b["index"] * block_size)
                out.write(data)
            print(f"  bloque {b['index']} <- {replica['host']}:{replica['port']} OK"
                  + (" (checksum verificado)" if b.get("checksum") else ""))
    if failed:
        os.remove(args.local_path)
        print("Descarga FALLIDA.")
        sys.exit(1)
    elapsed = time.time() - start
    print(f"Archivo descargado correctamente en {elapsed:.2f} s "
          f"({info['size'] / max(elapsed, 1e-6) / 1024 / 1024:.2f} MB/s).")


# ----------------------------------------------------------------------------
# RF3: acceso a archivos (open/read/write/seek/lock/close) usando el SDK
# ----------------------------------------------------------------------------

def sdk_from_session(session):
    auth_headers(session)  # exige login
    return DFSha(session.get("urls") or resolve_urls(DEFAULT_URLS), token=session["token"],
                 ca_cert=CA_CERT, workers=WORKERS, active_index=session.get("active_index", 0))


def remember_active(session, fs):
    if fs.active_index != session.get("active_index", 0):
        print(f"(failover: ahora se usa el ControlNode {fs.urls[fs.active_index]})", file=sys.stderr)
        session["active_index"] = fs.active_index
        save_session(session)


def cmd_cat(args, session):
    """Lee un rango del archivo trayendo SOLO los bloques necesarios."""
    fs = sdk_from_session(session)
    try:
        with fs.open(resolve_path(session, args.path), "r") as f:
            f.seek(args.offset)
            data = f.read(args.length)
    except DFShaError as exc:
        print("Error:", exc)
        return
    finally:
        remember_active(session, fs)
    sys.stdout.buffer.write(data)
    if sys.stdout.isatty() and not data.endswith(b"\n"):
        print()


def cmd_write(args, session):
    """Escribe en una posicion del archivo (lo crea si no existe)."""
    fs = sdk_from_session(session)
    data = open(args.from_file, "rb").read() if args.from_file else " ".join(args.text).encode()
    try:
        with fs.open(resolve_path(session, args.path), "rw", create=True) as f:
            f.seek(0, 2) if args.offset == "end" else f.seek(int(args.offset))
            f.write(data)
        print(f"OK: {len(data)} bytes escritos; el archivo ahora mide {f.size} bytes (version {f.version}).")
    except DFShaError as exc:
        print("Error:", exc)
    finally:
        remember_active(session, fs)


SHELL_HELP = """Comandos del shell DFSha:
  open <ruta> [r|rw] [create]   abre un archivo y devuelve un descriptor (fd)
  read <fd> [n]                 lee n bytes (o hasta el final) desde la posicion actual
  write <fd> <texto...>         escribe en la posicion actual (requiere rw)
  seek <fd> <pos|end>           mueve la posicion
  lock <fd> [shared|exclusive]  toma un lock;  unlock <fd>  lo suelta
  flush <fd>                    sube y confirma lo escrito sin cerrar
  close <fd>                    flush + cierra (libera el lock)
  files                         lista los archivos abiertos
  ls, cd, pwd, mkdir, rm, put, get, ...   igual que en la linea de comandos
  exit"""


def cmd_shell(args, session):
    fs = sdk_from_session(session)
    parser = build_parser()
    fds, next_fd = {}, 3
    print("Shell DFSha (escribe 'help'). Usuario:", session.get("username"))
    while True:
        try:
            line = input(f"dfsha:{session.get('cwd', '/')}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            line = "exit"
        if not line:
            continue
        parts = shlex.split(line)
        cmd, rest = parts[0], parts[1:]
        try:
            if cmd in ("exit", "quit"):
                for f in fds.values():
                    f.close()
                return
            elif cmd == "help":
                print(SHELL_HELP)
            elif cmd == "open":
                mode = rest[1] if len(rest) > 1 else "r"
                f = fs.open(resolve_path(session, rest[0]), mode, create="create" in rest[2:])
                fds[next_fd] = f
                print(f"fd {next_fd}: {f}")
                next_fd += 1
            elif cmd == "files":
                for fd, f in fds.items():
                    print(f"  fd {fd}: {f}")
            elif cmd in ("read", "write", "seek", "lock", "unlock", "flush", "close"):
                fd = int(rest[0])
                if fd not in fds:
                    print(f"el fd {fd} no existe (usa 'files' para ver los abiertos)")
                    continue
                f = fds[fd]
                if cmd == "read":
                    data = f.read(int(rest[1]) if len(rest) > 1 else -1)
                    print(repr(data.decode("utf-8", errors="replace")))
                elif cmd == "write":
                    print(f"{f.write(' '.join(rest[1:]).encode())} bytes en el buffer (se suben con flush/close)")
                elif cmd == "seek":
                    print("posicion", f.seek(0, 2) if rest[1] == "end" else f.seek(int(rest[1])))
                elif cmd == "lock":
                    print("lock:", f.lock(rest[1] if len(rest) > 1 else "exclusive"))
                elif cmd == "unlock":
                    f.unlock()
                    print("lock liberado")
                elif cmd == "flush":
                    f.flush()
                    print(f"confirmado: {f}")
                elif cmd == "close":
                    f.close()
                    del fds[fd]
                    print(f"fd {fd} cerrado (version {f.version}, {f.size} bytes)")
            elif cmd == "shell":
                print("ya estas en el shell")
            else:
                sub = parser.parse_args(parts)
                COMMANDS[sub.command](sub, session)
        except DFShaError as exc:
            print("Error:", exc)
        except (KeyError, IndexError, ValueError) as exc:
            print("Uso invalido:", exc, "(escribe 'help')")
        except SystemExit as exc:
            if exc.code not in (0, None):
                print(exc.code if isinstance(exc.code, str) else "comando fallido")
        remember_active(session, fs)


def build_parser():
    parser = argparse.ArgumentParser(description="Cliente DFSha")
    parser.add_argument("--url", default=os.environ.get("DFSHA_CONTROLNODE_URL", DEFAULT_URLS),
                        help="ControlNodes separados por coma (solo para register/login)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("register"); p.add_argument("username"); p.add_argument("password")
    p = sub.add_parser("login"); p.add_argument("username"); p.add_argument("password")
    sub.add_parser("pwd")
    p = sub.add_parser("cd"); p.add_argument("path", nargs="?", default="/")
    p = sub.add_parser("mkdir"); p.add_argument("path")
    p = sub.add_parser("rmdir"); p.add_argument("path")
    p = sub.add_parser("ls"); p.add_argument("path", nargs="?", default=".")
    p = sub.add_parser("rm"); p.add_argument("path")
    p = sub.add_parser("put"); p.add_argument("local_path"); p.add_argument("remote_path")
    p = sub.add_parser("get"); p.add_argument("remote_path"); p.add_argument("local_path")
    # RF2 con los nombres del enunciado
    p = sub.add_parser("send", help="= put"); p.add_argument("local_path"); p.add_argument("remote_path")
    p = sub.add_parser("receive", help="= get"); p.add_argument("remote_path"); p.add_argument("local_path")
    # RF3
    p = sub.add_parser("cat", help="lee un rango de un archivo")
    p.add_argument("path"); p.add_argument("--offset", type=int, default=0); p.add_argument("--length", type=int, default=-1)
    p = sub.add_parser("write", help="escribe en una posicion de un archivo")
    p.add_argument("path"); p.add_argument("offset", help="byte inicial, o 'end' para agregar al final")
    p.add_argument("text", nargs="*"); p.add_argument("--from-file", dest="from_file")
    sub.add_parser("shell", help="shell interactivo con open/read/write/lock/close")
    return parser


COMMANDS = {
    "register": cmd_register, "login": cmd_login, "pwd": cmd_pwd, "cd": cmd_cd,
    "mkdir": cmd_mkdir, "rmdir": cmd_rmdir, "ls": cmd_ls, "rm": cmd_rm,
    "put": cmd_put, "get": cmd_get, "send": cmd_put, "receive": cmd_get,
    "cat": cmd_cat, "write": cmd_write, "shell": cmd_shell,
}


def main():
    args = build_parser().parse_args()
    COMMANDS[args.command](args, load_session())


if __name__ == "__main__":
    main()
