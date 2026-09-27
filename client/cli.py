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

import os
import sys
import json
import argparse

import requests

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


def auth_headers(session):
    token = session.get("token")
    if not token:
        print("No has iniciado sesion. Usa: cli.py login <usuario> <password>")
        sys.exit(1)
    return {"Authorization": f"Bearer {token}"}


def cmd_register(args, session):
    url = args.url
    r = requests.post(f"{url}/auth/register", json={"username": args.username, "password": args.password})
    if r.ok:
        print(f"Usuario '{args.username}' creado.")
    else:
        print("Error:", r.json().get("error"))


def cmd_login(args, session):
    url = args.url
    r = requests.post(f"{url}/auth/login", json={"username": args.username, "password": args.password})
    if r.ok:
        token = r.json()["token"]
        save_session({"url": url, "token": token, "username": args.username})
        print(f"Sesion iniciada como {args.username}.")
    else:
        print("Error:", r.json().get("error"))


def cmd_mkdir(args, session):
    url = session["url"]
    r = requests.post(f"{url}/fs/mkdir", json={"path": args.path}, headers=auth_headers(session))
    print("OK" if r.ok else f"Error: {r.json().get('error')}")


def cmd_rmdir(args, session):
    url = session["url"]
    r = requests.delete(f"{url}/fs/rmdir", params={"path": args.path}, headers=auth_headers(session))
    print("OK" if r.ok else f"Error: {r.json().get('error')}")


def cmd_ls(args, session):
    url = session["url"]
    r = requests.get(f"{url}/fs/ls", params={"path": args.path}, headers=auth_headers(session))
    if not r.ok:
        print("Error:", r.json().get("error"))
        return
    for e in r.json()["entries"]:
        if e["type"] == "dir":
            print(f"  [dir]  {e['name']}")
        else:
            print(f"  [file] {e['name']}\t{e['size']} bytes\t{e['status']}")


def cmd_rm(args, session):
    url = session["url"]
    r = requests.delete(f"{url}/fs/rm", params={"path": args.path}, headers=auth_headers(session))
    print("OK" if r.ok else f"Error: {r.json().get('error')}")


def cmd_put(args, session):
    url = session["url"]
    size = os.path.getsize(args.local_path)

    r = requests.post(f"{url}/files/put/init", json={"path": args.remote_path, "size": size},
                       headers=auth_headers(session))
    if not r.ok:
        print("Error:", r.json().get("error"))
        return
    info = r.json()
    block_size = info["block_size"]
    blocks = info["blocks"]
    print(f"Subiendo '{args.local_path}' ({size} bytes) en {len(blocks)} bloque(s), "
          f"replicacion x{len(blocks[0]['replicas']) if blocks else 0}...")

    with open(args.local_path, "rb") as f:
        for b in blocks:
            f.seek(b["index"] * block_size)
            chunk = f.read(block_size)
            for replica in b["replicas"]:
                blockproto.put_block(replica["host"], replica["port"], b["block_id"], chunk)
            print(f"  bloque {b['index']} -> {[r['host'] + ':' + str(r['port']) for r in b['replicas']]} OK")

    r = requests.post(f"{url}/files/put/complete", json={"path": args.remote_path}, headers=auth_headers(session))
    print("Archivo subido correctamente." if r.ok else f"Error al finalizar: {r.json().get('error')}")


def cmd_get(args, session):
    url = session["url"]
    r = requests.get(f"{url}/files/get", params={"path": args.remote_path}, headers=auth_headers(session))
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
            for replica in b["replicas"]:
                try:
                    data = blockproto.get_block(replica["host"], replica["port"], b["block_id"])
                    print(f"  bloque {b['index']} <- {replica['host']}:{replica['port']} OK")
                    break
                except Exception as exc:
                    last_err = exc
                    continue
            if data is None:
                print(f"  bloque {b['index']}: todas las replicas fallaron ({last_err})")
                sys.exit(1)
            out.write(data)
    print("Archivo descargado correctamente.")


def main():
    parser = argparse.ArgumentParser(description="Cliente DFSha")
    parser.add_argument("--url", default=os.environ.get("DFSHA_CONTROLNODE_URL", "http://127.0.0.1:5000"))
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
    if args.command in ("register", "login"):
        args.url = session.get("url", args.url) if args.command != "login" else args.url

    dispatch = {
        "register": cmd_register, "login": cmd_login, "mkdir": cmd_mkdir,
        "rmdir": cmd_rmdir, "ls": cmd_ls, "rm": cmd_rm, "put": cmd_put, "get": cmd_get,
    }
    dispatch[args.command](args, session)


if __name__ == "__main__":
    main()
