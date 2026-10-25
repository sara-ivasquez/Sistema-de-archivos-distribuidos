"""
Protocolo de transferencia de bloques del cliente DFSha (Cliente <-> DataNode).

Implementa sobre TCP + TLS el contrato descrito en proto/block_service.proto.
El servidor es datanode_go/main.go; el ControlNode (controlnode_go/main.go)
usa este mismo framing para DELETE y REPLICATE, y los DataNodes entre si
para PUT_RAW.

Formato de un REQUEST (big-endian / network byte order):
    1 byte   -> opcode (1=PUT, 2=GET, 3=DELETE, 4=REPLICATE, 5=PUT_RAW)
    4 bytes  -> longitud del block_id (uint32)
    N bytes  -> block_id (solo [A-Za-z0-9_-])
    [DELETE/REPLICATE/PUT_RAW] 2 bytes len(secreto) + secreto del cluster
    [PUT/PUT_RAW] 8 bytes len(datos) (uint64) + datos crudos del bloque
    [REPLICATE]   2 bytes len(host) + host destino + 2 bytes puerto destino

Formato de una RESPONSE:
    1 byte   -> status (1=ok, 0=error)
    4 bytes  -> longitud del payload (uint32)
    K bytes  -> payload (bytes del bloque en GET ok; mensaje de texto si no)

El cliente solo usa PUT y GET. DELETE, REPLICATE y PUT_RAW exigen el
secreto del cluster, asi que un cliente no puede borrar ni copiar bloques.
"""

import os
import socket
import ssl
import struct
import time

OP_PUT = 1
OP_GET = 2

STATUS_OK = 1
STATUS_ERROR = 0

DEFAULT_CA = os.path.join(os.path.dirname(__file__), "..", "certs", "cert.pem")
_ssl_context = None

# Tiempos pensados para Internet (cliente en casa -> VMs en la nube), no solo
# para una red local: el plazo de cada transferencia crece con el tamano del
# bloque, y un fallo transitorio se reintenta antes de darlo por perdido.
CONNECT_TIMEOUT = float(os.environ.get("DFSHA_CONNECT_TIMEOUT", "10"))
MIN_RATE = float(os.environ.get("DFSHA_MIN_RATE_KBPS", "64")) * 1024  # bytes/s minimos esperados
GET_TIMEOUT = float(os.environ.get("DFSHA_BLOCK_TIMEOUT", "120"))
RETRIES = int(os.environ.get("DFSHA_RETRIES", "2"))


def ssl_context() -> ssl.SSLContext:
    """Contexto TLS que confia solo en el certificado del cluster DFSha."""
    global _ssl_context
    if _ssl_context is None:
        ca = os.environ.get("DFSHA_CA_CERT", DEFAULT_CA)
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        _ssl_context = ctx
    return _ssl_context


def _connect(host: str, port: int, timeout: float) -> ssl.SSLSocket:
    raw = socket.create_connection((host, port), timeout=CONNECT_TIMEOUT)
    try:
        sock = ssl_context().wrap_socket(raw, server_hostname=host)
        sock.settimeout(timeout)  # plazo para la transferencia completa
        return sock
    except Exception:
        raw.close()
        raise


def _with_retries(fn):
    """Reintenta errores de red transitorios (timeout, conexion caida).
    PUT es idempotente (mismo block_id), asi que repetirlo es seguro."""
    last = None
    for attempt in range(RETRIES + 1):
        try:
            return fn()
        except (socket.timeout, TimeoutError, ConnectionError, ssl.SSLError, OSError) as exc:
            if isinstance(exc, ssl.SSLCertVerificationError):
                raise  # un certificado invalido no se arregla reintentando
            last = exc
            if attempt < RETRIES:
                time.sleep(1 + attempt)
    raise last


def _recv_exact(sock, n: int) -> bytes:
    """Lee exactamente n bytes del socket (TCP puede entregar datos en partes)."""
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(min(remaining, 1 << 20))
        if not chunk:
            raise ConnectionError("Conexion cerrada antes de completar el mensaje")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _send_request(sock, opcode: int, block_id: str, data: bytes = b""):
    block_id_bytes = block_id.encode("utf-8")
    header = struct.pack("!BI", opcode, len(block_id_bytes)) + block_id_bytes
    if opcode == OP_PUT:
        header += struct.pack("!Q", len(data))
    sock.sendall(header)
    if opcode == OP_PUT and data:
        sock.sendall(data)


def _recv_response(sock):
    status, payload_len = struct.unpack("!BI", _recv_exact(sock, 5))
    payload = _recv_exact(sock, payload_len) if payload_len else b""
    return status, payload


def put_block(host: str, port: int, block_id: str, data: bytes, timeout: float = None) -> bool:
    if timeout is None:
        timeout = 30 + len(data) / MIN_RATE  # 4 MB -> ~94 s

    def attempt():
        with _connect(host, port, timeout) as sock:
            _send_request(sock, OP_PUT, block_id, data)
            return _recv_response(sock)
    status, payload = _with_retries(attempt)
    if status != STATUS_OK:
        raise RuntimeError(f"PutBlock fallo en {host}:{port} -> {payload.decode(errors='replace')}")
    return True


def get_block(host: str, port: int, block_id: str, timeout: float = None) -> bytes:
    def attempt():
        with _connect(host, port, timeout or GET_TIMEOUT) as sock:
            _send_request(sock, OP_GET, block_id)
            return _recv_response(sock)
    status, payload = _with_retries(attempt)
    if status != STATUS_OK:
        raise RuntimeError(f"GetBlock fallo en {host}:{port} -> {payload.decode(errors='replace')}")
    return payload
