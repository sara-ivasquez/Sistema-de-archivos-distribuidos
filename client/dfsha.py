"""
SDK de DFSha para Python — servicio de ACCESO a archivos (RF3).

Permite usar un archivo de DFSha casi como un archivo local:

    from client.dfsha import DFSha

    fs = DFSha(["https://cn1:5000", "https://cn2:5000"])
    fs.login("alice", "clave123")

    with fs.open("/docs/notas.txt", "rw", create=True) as f:
        f.write(b"hola mundo")
        f.seek(0)
        print(f.read(4))              # b'hola'

    with fs.open("/docs/notas.txt") as f:      # solo lectura
        f.lock("shared")              # nadie puede escribir mientras leo
        f.seek(5)
        print(f.read())               # b'mundo'

Como funciona (ver controlnode_go/acceso.go):
  - open() pide al ControlNode un handle y la lista de bloques del archivo.
  - read() trae de los DataNodes SOLO los bloques del rango pedido (directo,
    en paralelo, verificando el checksum de cada bloque).
  - write() modifica los bloques en memoria (cache de escritura). flush() o
    close() los suben como bloques NUEVOS con quorum y el ControlNode los
    reemplaza de forma atomica (copy-on-write).
  - open(..., "rw") toma el lock exclusivo: un solo escritor a la vez. Los
    lectores pueden tomar un lock compartido. Si el archivo esta bloqueado,
    se lanza FileLockedError.
  - Mientras el archivo este abierto, un hilo renueva el lease del handle;
    si el cliente muere, el lock se libera solo al expirar el lease.
  - El handle se replica al ControlNode standby: si el primario cae, el
    archivo sigue abierto (failover transparente).
"""
import hashlib
import io
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import blockproto  # noqa: E402


class DFShaError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class FileLockedError(DFShaError):
    pass


class DFSha:
    """Conexion con el cluster (lista de ControlNodes con failover)."""

    def __init__(self, urls, token=None, ca_cert=None, workers=None, active_index=0):
        if isinstance(urls, str):
            urls = [u.strip() for u in urls.split(",") if u.strip()]
        self.urls = [u.rstrip("/") for u in urls]
        self.token = token
        self.ca_cert = ca_cert or os.environ.get("DFSHA_CA_CERT", blockproto.DEFAULT_CA)
        self.workers = workers or int(os.environ.get("DFSHA_WORKERS", "4"))
        self.active_index = active_index
        self._open_files = set()
        self._renew_stop = threading.Event()
        self._renew_thread = None

    # -------------------------------------------------------------- HTTP --
    def request(self, method, path, **kwargs):
        """Llama al ControlNode activo; si no responde o esta en standby
        (503), prueba el siguiente. Lanza DFShaError si la respuesta es error."""
        headers = kwargs.pop("headers", {})
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        last_err = None
        n = len(self.urls)
        for offset in range(n):
            i = (self.active_index + offset) % n
            try:
                r = requests.request(method, self.urls[i] + path, headers=headers,
                                     timeout=15, verify=self.ca_cert, **kwargs)
            except requests.exceptions.SSLError:
                raise
            except requests.exceptions.RequestException as exc:
                last_err = exc
                continue
            if r.status_code == 503:
                last_err = f"{self.urls[i]} esta en standby"
                continue
            self.active_index = i
            if not r.ok:
                try:
                    msg = r.json().get("error", r.text)
                except ValueError:
                    msg = r.text
                cls = FileLockedError if r.status_code == 423 else DFShaError
                raise cls(r.status_code, msg)
            return r.json()
        raise DFShaError(0, f"ningun ControlNode disponible: {last_err}")

    def login(self, username, password):
        self.token = self.request("POST", "/auth/login", json={"username": username, "password": password})["token"]
        return self.token

    # ------------------------------------------------- acceso a archivos --
    def open(self, path, mode="r", create=False):
        """Abre un archivo. mode: "r" (lectura) o "rw" (lectura/escritura,
        toma el lock exclusivo). create=True crea el archivo si no existe."""
        info = self.request("POST", "/files/open", json={"path": path, "mode": mode, "create": create})
        f = DFSFile(self, path, info)
        self._open_files.add(f)
        self._start_renewal(info.get("lease_seconds", 60))
        return f

    def _start_renewal(self, lease_seconds):
        if self._renew_thread and self._renew_thread.is_alive():
            return
        interval = max(lease_seconds / 3, 1)

        def loop():
            while not self._renew_stop.wait(interval):
                for f in list(self._open_files):
                    try:
                        self.request("POST", "/files/renew", json={"handle": f.handle})
                    except DFShaError:
                        pass
        self._renew_thread = threading.Thread(target=loop, daemon=True)
        self._renew_thread.start()


class DFSFile(io.RawIOBase):
    """Archivo abierto en DFSha (devuelto por DFSha.open)."""

    def __init__(self, fs, path, info):
        super().__init__()
        self.fs = fs
        self.path = path
        self.handle = info["handle"]
        self.mode = info["mode"]
        self.lock_type = info["lock"]
        self.version = info.get("version", 0)
        self.block_size = info["block_size"]
        self.size = info["size"]
        self._committed_size = info["size"]
        self._blocks = {b["index"]: b for b in info["blocks"]}
        self._cache = {}   # indice -> bytes (bloques ya leidos y verificados)
        self._dirty = {}   # indice -> bytearray (bloques modificados, sin subir)
        self._pos = 0

    # ----------------------------------------------------- io.RawIOBase --
    def readable(self):
        return True

    def writable(self):
        return self.mode == "rw"

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            new = offset
        elif whence == io.SEEK_CUR:
            new = self._pos + offset
        elif whence == io.SEEK_END:
            new = self.size + offset
        else:
            raise ValueError("whence invalido")
        if new < 0:
            raise ValueError("posicion negativa")
        self._pos = new
        return new

    # ------------------------------------------------------------ bloques --
    def _old_len(self, i):
        return max(0, min(self.block_size, self._committed_size - i * self.block_size))

    def _fetch(self, i):
        """Bytes del bloque i (dirty, en cache, o leido de un DataNode)."""
        if i in self._dirty:
            return bytes(self._dirty[i])
        if i in self._cache:
            return self._cache[i]
        b = self._blocks.get(i)
        if b is None:
            return b""
        errors = []
        for rep in b["replicas"]:
            try:
                data = blockproto.get_block(rep["host"], rep["port"], b["block_id"])
                if b.get("checksum") and hashlib.sha256(data).hexdigest() != b["checksum"]:
                    raise ValueError("checksum no coincide")
                self._cache[i] = data
                return data
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{rep['host']}:{rep['port']} ({exc})")
        raise DFShaError(0, f"no se pudo leer el bloque {i}: " + "; ".join(errors))

    def _prefetch(self, indices):
        missing = [i for i in indices if i not in self._dirty and i not in self._cache and i in self._blocks]
        if len(missing) > 1:
            with ThreadPoolExecutor(max_workers=self.fs.workers) as pool:
                list(pool.map(self._fetch, missing))

    # ------------------------------------------------------- read / write --
    def read(self, n=-1):
        self._check_open()
        end = self.size if n is None or n < 0 else min(self.size, self._pos + n)
        if self._pos >= end:
            return b""
        bs = self.block_size
        first, last = self._pos // bs, (end - 1) // bs
        self._prefetch(range(first, last + 1))
        out = bytearray()
        for i in range(first, last + 1):
            data = self._fetch(i)
            start = max(self._pos, i * bs) - i * bs
            stop = min(end, (i + 1) * bs) - i * bs
            chunk = data[start:stop]
            out += chunk + b"\0" * ((stop - start) - len(chunk))  # huecos = ceros
        self._pos = end
        return bytes(out)

    def readall(self):
        return self.read(-1)

    def readinto(self, b):
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)

    def write(self, data):
        self._check_open()
        if self.mode != "rw":
            raise DFShaError(403, "el archivo se abrio solo para lectura")
        data = bytes(data)
        if not data:
            return 0
        bs = self.block_size
        end = self._pos + len(data)
        first, last = self._pos // bs, (end - 1) // bs
        # Los bloques que se escriben solo en parte hay que leerlos primero.
        self._prefetch([i for i in (first, last)
                        if not (self._pos <= i * bs and end >= (i + 1) * bs)])
        written = 0
        for i in range(first, last + 1):
            buf = self._dirty.get(i)
            if buf is None:
                buf = bytearray(self._fetch(i))
            start = max(self._pos, i * bs) - i * bs
            stop = min(end, (i + 1) * bs) - i * bs
            if len(buf) < stop:
                buf.extend(b"\0" * (stop - len(buf)))
            buf[start:stop] = data[written:written + (stop - start)]
            written += stop - start
            self._dirty[i] = buf
        self._pos = end
        self.size = max(self.size, end)
        return len(data)

    def flush(self):
        """Sube los bloques modificados y los confirma en el ControlNode."""
        if self.closed or self.mode != "rw":
            return
        if not self._dirty and self.size == self._committed_size:
            return
        bs = self.block_size
        n = (self.size + bs - 1) // bs
        # Bloques que cambian de largo (el viejo ultimo bloque, o huecos al
        # escribir mas alla del final) tambien se reescriben.
        for i in range(n):
            exp_len = min(bs, self.size - i * bs)
            if i not in self._dirty and self._old_len(i) != exp_len:
                self._dirty[i] = bytearray(self._fetch(i))
        for i, buf in self._dirty.items():
            exp_len = min(bs, self.size - i * bs)
            if len(buf) < exp_len:
                buf.extend(b"\0" * (exp_len - len(buf)))

        indices = sorted(self._dirty)
        alloc = self.fs.request("POST", "/files/write/alloc", json={"handle": self.handle, "indices": indices})
        quorum = alloc["write_quorum"]

        def upload(b):
            data = bytes(self._dirty[b["index"]])
            ok, errs = 0, []
            for rep in b["replicas"]:
                try:
                    blockproto.put_block(rep["host"], rep["port"], b["block_id"], data)
                    ok += 1
                except Exception as exc:  # noqa: BLE001
                    errs.append(f"{rep['host']}:{rep['port']} ({exc})")
            if ok < quorum:
                raise DFShaError(0, f"bloque {b['index']}: quorum no alcanzado ({'; '.join(errs)})")
            return b, data, hashlib.sha256(data).hexdigest()

        with ThreadPoolExecutor(max_workers=self.fs.workers) as pool:
            results = list(pool.map(upload, alloc["blocks"]))
        checksums = {b["block_id"]: cs for b, _, cs in results}
        resp = self.fs.request("POST", "/files/flush",
                               json={"handle": self.handle, "size": self.size, "checksums": checksums})
        for b, data, cs in results:
            b["checksum"] = cs
            self._blocks[b["index"]] = b
            self._cache[b["index"]] = data
        self._dirty.clear()
        self._committed_size = self.size
        self.version = resp.get("version", self.version)

    def close(self):
        if self.closed:
            return
        try:
            self.flush()
        finally:
            try:
                self.fs.request("POST", "/files/close", json={"handle": self.handle})
            finally:
                self.fs._open_files.discard(self)
                super().close()

    # -------------------------------------------------------------- locks --
    def lock(self, kind="exclusive"):
        """kind: "shared" (varios lectores, ningun escritor) o "exclusive"."""
        self._check_open()
        self.lock_type = self.fs.request("POST", "/files/lock", json={"handle": self.handle, "type": kind})["lock"]
        return self.lock_type

    def unlock(self):
        self._check_open()
        self.lock_type = self.fs.request("POST", "/files/unlock", json={"handle": self.handle})["lock"]

    def _check_open(self):
        if self.closed:
            raise ValueError("el archivo esta cerrado")

    def __repr__(self):
        return (f"<DFSFile {self.path} mode={self.mode} lock={self.lock_type or '-'} "
                f"size={self.size} pos={self._pos} v{self.version}>")
