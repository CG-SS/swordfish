"""A minimal websocket server that records the data frames it receives.

Used by tests/test_http_check.sh. It exists because the two things most worth
asserting about our websocket output cannot be seen through another pipeline:
whether the LAST message actually reached the wire (seastar batches its socket
flushes, so a teardown can discard it after the pipeline has already acked it),
and whether a message larger than 512 bytes arrives as ONE frame (writing
through seastar's websocket output_stream splits it into several).

A peer implementation would answer neither: it reassembles or buffers, and a
lost frame looks the same as a slow one.

Dependency-free on purpose: no websocket library is installed here, and one more
moving part between the pipeline and the assertion is one more thing that can be
blamed for a failure.

Usage: ws_probe_server.py <port> <outfile>
"""
import base64
import hashlib
import socket
import struct
import sys

GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def handshake(conn):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            return False
        data += chunk
    key = None
    for line in data.split(b"\r\n"):
        if line.lower().startswith(b"sec-websocket-key:"):
            key = line.split(b":", 1)[1].strip()
    if key is None:
        return False
    accept = base64.b64encode(hashlib.sha1(key + GUID).digest())
    conn.sendall(
        b"HTTP/1.1 101 Switching Protocols\r\n"
        b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
        b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n"
    )
    return True


def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def read_frame(conn):
    """Returns (opcode, payload) or None at end of stream."""
    hdr = recv_exact(conn, 2)
    if hdr is None:
        return None
    opcode = hdr[0] & 0x0F
    masked = bool(hdr[1] & 0x80)
    length = hdr[1] & 0x7F
    if length == 126:
        ext = recv_exact(conn, 2)
        if ext is None:
            return None
        length = struct.unpack(">H", ext)[0]
    elif length == 127:
        ext = recv_exact(conn, 8)
        if ext is None:
            return None
        length = struct.unpack(">Q", ext)[0]
    mask = recv_exact(conn, 4) if masked else None
    if masked and mask is None:
        return None
    payload = recv_exact(conn, length) if length else b""
    if payload is None:
        return None
    if masked:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def main():
    port, out = int(sys.argv[1]), sys.argv[2]
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(4)
    with open(out, "w") as f:
        while True:
            conn, _ = srv.accept()
            try:
                if not handshake(conn):
                    conn.close()
                    continue
                while True:
                    frame = read_frame(conn)
                    if frame is None:
                        break
                    opcode, payload = frame
                    if opcode == 0x8:          # CLOSE
                        break
                    if opcode in (0x1, 0x2, 0x0):
                        f.write(payload.decode("utf-8", "replace") + "\n")
                        f.flush()
            finally:
                conn.close()


if __name__ == "__main__":
    main()
