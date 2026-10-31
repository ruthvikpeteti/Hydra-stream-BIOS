#!/usr/bin/env python3
"""Minimal Kafka protocol probe: sends ApiVersions v0 to host:port and checks for a valid response.

Used to prove that the EXTERNAL (host-facing) listener of a broker speaks Kafka. Exit 0 on success.
Usage: kafka_ping.py <host> <port>
"""

import socket
import struct
import sys

host, port = sys.argv[1], int(sys.argv[2])
cid = b"ss-ping"
corr = 7
body = struct.pack(">hhih", 18, 0, corr, len(cid)) + cid  # api_key=18, version=0
try:
    with socket.create_connection((host, port), timeout=5) as s:
        s.sendall(struct.pack(">i", len(body)) + body)
        hdr = s.recv(4)
        size = struct.unpack(">i", hdr)[0]
        data = b""
        while len(data) < size:
            chunk = s.recv(size - len(data))
            if not chunk:
                break
            data += chunk
    got_corr, err = struct.unpack(">ih", data[:6])
    ok = got_corr == corr and err == 0
except Exception as e:  # noqa: BLE001
    print(f"{host}:{port} unreachable: {e}")
    sys.exit(1)
print(f"{host}:{port} ApiVersions error_code={err}")
sys.exit(0 if ok else 1)
