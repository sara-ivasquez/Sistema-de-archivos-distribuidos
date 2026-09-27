"""
Protocolo de transferencia de bloques (Cliente<->DataNode y DataNode<->DataNode).

Este modulo implementa, sobre sockets TCP puros, el MISMO contrato de mensajes
definido en proto/block_service.proto (PutBlock, GetBlock, DeleteBlock). Se usa
como referencia funcional porque el entorno de pruebas no tenia acceso a
internet para instalar grpcio; la migracion a gRPC real es directa (ver
README.md, seccion "Migrar a gRPC real") porque los mensajes y la semantica
son identicos.

Formato de un REQUEST (big-endian / network byte order):
    1 byte   -> opcode (1=PUT, 2=GET, 3=DELETE)
    4 bytes  -> longitud del block_id (uint32)
    N bytes  -> block_id (utf-8)
    [solo PUT] 8 bytes -> longitud de los datos (uint64)
    [solo PUT] M bytes -> datos crudos del bloque

Formato de una RESPONSE:
    1 byte   -> status (1=ok, 0=error)
    4 bytes  -> longitud del payload (uint32)
    K bytes  -> payload:
                  - PUT/DELETE: mensaje de texto (utf-8)
                  - GET (ok): bytes crudos del bloque
                  - GET (error): mensaje de texto (utf-8)
"""

import socket
import struct

OP_PUT = 1
OP_GET = 2
OP_DELETE = 3

STATUS_OK = 1
STATUS_ERROR = 0


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Lee exactamente n bytes del socket (TCP puede entregar datos en partes)."""
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("Conexion cerrada antes de completar el mensaje")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def send_request(sock: socket.socket, opcode: int, block_id: str, data: bytes = b""):
    block_id_bytes = block_id.encode("utf-8")
    header = struct.pack("!BI", opcode, len(block_id_bytes)) + block_id_bytes
    if opcode == OP_PUT:
        header += struct.pack("!Q", len(data))
    sock.sendall(header)
    if opcode == OP_PUT and data:
        sock.sendall(data)


def recv_request(sock: socket.socket):
    """Usado por el servidor (DataNode) para leer una solicitud entrante."""
    opcode, block_id_len = struct.unpack("!BI", _recv_exact(sock, 5))
    block_id = _recv_exact(sock, block_id_len).decode("utf-8")
    data = b""
    if opcode == OP_PUT:
        (data_len,) = struct.unpack("!Q", _recv_exact(sock, 8))
        data = _recv_exact(sock, data_len)
    return opcode, block_id, data


def send_response(sock: socket.socket, status: int, payload: bytes):
    sock.sendall(struct.pack("!BI", status, len(payload)) + payload)


def recv_response(sock: socket.socket):
    status, payload_len = struct.unpack("!BI", _recv_exact(sock, 5))
    payload = _recv_exact(sock, payload_len) if payload_len else b""
    return status, payload


def put_block(host: str, port: int, block_id: str, data: bytes, timeout: float = 10.0) -> bool:
    with socket.create_connection((host, port), timeout=timeout) as sock:
        send_request(sock, OP_PUT, block_id, data)
        status, payload = recv_response(sock)
        if status != STATUS_OK:
            raise RuntimeError(f"PutBlock fallo en {host}:{port} -> {payload.decode(errors='replace')}")
        return True


def get_block(host: str, port: int, block_id: str, timeout: float = 10.0) -> bytes:
    with socket.create_connection((host, port), timeout=timeout) as sock:
        send_request(sock, OP_GET, block_id)
        status, payload = recv_response(sock)
        if status != STATUS_OK:
            raise RuntimeError(f"GetBlock fallo en {host}:{port} -> {payload.decode(errors='replace')}")
        return payload


def delete_block(host: str, port: int, block_id: str, timeout: float = 5.0) -> bool:
    with socket.create_connection((host, port), timeout=timeout) as sock:
        send_request(sock, OP_DELETE, block_id)
        status, _ = recv_response(sock)
        return status == STATUS_OK
