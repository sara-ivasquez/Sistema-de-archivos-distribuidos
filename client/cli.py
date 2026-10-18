"""
Cliente DFSha (RF1, RF2, RF4).

Uso:
    python3 client/cli.py --url http://127.0.0.1:5000 register alice 1234
    python3 client/cli.py --url http://127.0.0.1:5000 login alice 1234
    python3 client/cli.py mkdir /docs
    python3 client/cli.py ls /
    python3 client/cli.py put ./informe.pdf /docs/informe.pdf
    python3 client/cli.py get /docs/informe.pdf ./descargado.pdf
    python3 client/cli.py rm /docs/informe.pdf
    python3 client/cli.py rmdir /docs

La sesion (token) se guarda en .dfsha_session.json en el directorio actual.
"""
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import os
import sys
import json
import hashlib
import argparse

import requests

CERT_PATH = os.path.join(os.path.dirname(__file__), "..", "certs", "cert.pem")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# DFSHA_USE_GRPC=1 -> usa el DataNode real por gRPC (requiere haber generado
# los stubs, ver README.md). Sin esa variable, usa la version por sockets TCP
# (funciona sin instalar nada extra). Misma interfaz en ambos casos.
if os.environ.get("DFSHA_USE_GRPC") == "1":
    from common import blockproto_grpc as blockproto  # noqa: E402
else:
    from common import blockproto  # noqa: E402

SESSION_FILE = ".dfsha_session.json"


def load_session():
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE) as f:
            return json.load(f)
    return {}


def save_session(data):
    with open(SESSION_FILE, "w") as f:
        json.dump(data, f)

def resolve_urls(raw):
    return [u.strip() for u in raw.split(",") if u.strip()]


def try_urls(urls, start_index, method, path, **kwargs):
    """Intenta cada URL de la lista empezando por start_index.
    Devuelve (response, indice_que_funciono). Si todas fallan, lanza error."""
    last_exc = None
    n = len(urls)
    for offset in range(n):
        i = (start_index + offset) % n
        url = urls[i]
        try:
            r = requests.request(method, f"{url}{path}", timeout=10, verify=False, **kwargs)
            return r, i
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            continue
    raise RuntimeError(f"Ningun ControlNode disponible en {urls}: {last_exc}")


def session_request(session, method, path, **kwargs):
    urls = session.get("urls") or resolve_urls(session.get("url", "http://127.0.0.1:5000"))
    idx = session.get("active_index", 0)
    r, idx = try_urls(urls, idx, method, path, **kwargs)
    session["urls"] = urls
    session["active_index"] = idx
    save_session(session)
    return r

def auth_headers(session):
    token = session.get("token")
    if not token:
        print("No has iniciado sesion. Usa: cli.py login <usuario> <password>")
        sys.exit(1)
    return {"Authorization": f"Bearer {token}"}


def cmd_register(args, session):
    urls = resolve_urls(args.url)
    r, idx = try_urls(urls, 0, "POST", "/auth/register", json={"username": args.username, "password": args.password})
    print(f"Usuario '{args.username}' creado." if r.ok else f"Error: {r.json().get('error')}")

def cmd_login(args, session):
    urls = resolve_urls(args.url)
    r, idx = try_urls(urls, 0, "POST", "/auth/login", json={"username": args.username, "password": args.password})
    if r.ok:
        token = r.json()["token"]
        save_session({"urls": urls, "active_index": idx, "token": token, "username": args.username})
        print(f"Sesion iniciada como {args.username} (ControlNode activo: {urls[idx]}).")
    else:
        print("Error:", r.json().get("error"))


def cmd_mkdir(args, session):
    r = session_request(session, "POST", "/fs/mkdir", json={"path": args.path}, headers=auth_headers(session))
    print("OK" if r.ok else f"Error: {r.json().get('error')}")

def cmd_rmdir(args, session):
    r = session_request(session, "DELETE", "/fs/rmdir", params={"path": args.path}, headers=auth_headers(session))
    print("OK" if r.ok else f"Error: {r.json().get('error')}")

def cmd_ls(args, session):
    r = session_request(session, "GET", "/fs/ls", params={"path": args.path}, headers=auth_headers(session))
    if not r.ok:
        print("Error:", r.json().get("error"))
        return
    for e in r.json()["entries"]:
        if e["type"] == "dir":
            print(f"  [dir]  {e['name']}")
        else:
            print(f"  [file] {e['name']}\t{e['size']} bytes\t{e['status']}")


def cmd_rm(args, session):
    r = session_request(session, "DELETE", "/fs/rm", params={"path": args.path}, headers=auth_headers(session))
    print("OK" if r.ok else f"Error: {r.json().get('error')}")

def cmd_put(args, session):
    size = os.path.getsize(args.local_path)

    r = session_request(session, "POST", "/files/put/init", json={"path": args.remote_path, "size": size},
                         headers=auth_headers(session))
    if not r.ok:
        print("Error:", r.json().get("error"))
        return
    info = r.json()
    block_size = info["block_size"]
    blocks = info["blocks"]
    write_quorum = info.get("write_quorum", len(blocks[0]["replicas"]) if blocks else 1)
    print(f"Subiendo '{args.local_path}' ({size} bytes) en {len(blocks)} bloque(s), "
          f"replicacion x{len(blocks[0]['replicas']) if blocks else 0}, quorum de escritura={write_quorum}...")

    checksums = {}
    with open(args.local_path, "rb") as f:
        for b in blocks:
            f.seek(b["index"] * block_size)
            chunk = f.read(block_size)
            checksums[b["block_id"]] = hashlib.sha256(chunk).hexdigest()

            successes = 0
            failures = []
            for replica in b["replicas"]:
                try:
                    blockproto.put_block(replica["host"], replica["port"], b["block_id"], chunk)
                    successes += 1
                except Exception as exc:
                    failures.append(f"{replica['host']}:{replica['port']} -> {exc}")

            status = "OK" if successes >= write_quorum else "QUORUM NO ALCANZADO"
            print(f"  bloque {b['index']}: {successes}/{len(b['replicas'])} replicas confirmaron "
                  f"(quorum={write_quorum}) [{status}]")
            if successes < write_quorum:
                print("  Fallas:", "; ".join(failures) if failures else "(ninguna replica disponible)")
                print("Subida ABORTADA: no se alcanzo el quorum de escritura para este bloque.")
                sys.exit(1)

    r = session_request(session, "POST", "/files/put/complete", json={"path": args.remote_path, "checksums": checksums},
                         headers=auth_headers(session))
    print("Archivo subido correctamente." if r.ok else f"Error al finalizar: {r.json().get('error')}")


def cmd_get(args, session):
    r = session_request(session, "GET", "/files/get", params={"path": args.remote_path}, headers=auth_headers(session))
    if not r.ok:
        print("Error:", r.json().get("error"))
        return
    info = r.json()
    blocks = sorted(info["blocks"], key=lambda b: b["index"])
    print(f"Descargando '{args.remote_path}' ({info['size']} bytes) en {len(blocks)} bloque(s)...")

    with open(args.local_path, "wb") as out:
        for b in blocks:
            data = None
            last_err = None
            expected_checksum = b.get("checksum")
            for replica in b["replicas"]:
                try:
                    candidate = blockproto.get_block(replica["host"], replica["port"], b["block_id"])
                    if expected_checksum:
                        actual = hashlib.sha256(candidate).hexdigest()
                        if actual != expected_checksum:
                            raise ValueError(f"checksum no coincide (esperado {expected_checksum[:8]}…, "
                                              f"obtenido {actual[:8]}…)")
                    data = candidate
                    print(f"  bloque {b['index']} <- {replica['host']}:{replica['port']} OK"
                          + (" (checksum verificado)" if expected_checksum else ""))
                    break
                except Exception as exc:
                    last_err = exc
                    print(f"  bloque {b['index']}: replica {replica['host']}:{replica['port']} fallo ({exc}), probando siguiente...")
                    continue
            if data is None:
                print(f"  bloque {b['index']}: todas las replicas fallaron ({last_err})")
                sys.exit(1)
            out.write(data)
    print("Archivo descargado correctamente.")


def main():
    parser = argparse.ArgumentParser(description="Cliente DFSha")
    parser.add_argument("--url", default=os.environ.get("DFSHA_CONTROLNODE_URL", "https://127.0.0.1:5000,https://127.0.0.1:5001"))    
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("register"); p.add_argument("username"); p.add_argument("password")
    p = sub.add_parser("login"); p.add_argument("username"); p.add_argument("password")
    p = sub.add_parser("mkdir"); p.add_argument("path")
    p = sub.add_parser("rmdir"); p.add_argument("path")
    p = sub.add_parser("ls"); p.add_argument("path", nargs="?", default="/")
    p = sub.add_parser("rm"); p.add_argument("path")
    p = sub.add_parser("put"); p.add_argument("local_path"); p.add_argument("remote_path")
    p = sub.add_parser("get"); p.add_argument("remote_path"); p.add_argument("local_path")

    args = parser.parse_args()
    session = load_session()

    dispatch = {
        "register": cmd_register, "login": cmd_login, "mkdir": cmd_mkdir,
        "rmdir": cmd_rmdir, "ls": cmd_ls, "rm": cmd_rm, "put": cmd_put, "get": cmd_get,
    }
    dispatch[args.command](args, session)


if __name__ == "__main__":
    main()

