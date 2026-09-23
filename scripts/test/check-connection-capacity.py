#!/usr/bin/env python3
"""Check that the community TCP listener accepts 1,025 live MQTT clients."""

import socket
import time
from contextlib import ExitStack


HOST = "127.0.0.1"
PORT = 1883
CLIENTS = 1025


def read_exact(sock, count):
    chunks = []
    while count:
        chunk = sock.recv(count)
        if not chunk:
            raise RuntimeError("listener closed before CONNACK")
        chunks.append(chunk)
        count -= len(chunk)
    return b"".join(chunks)


def wait_for_listener():
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((HOST, PORT), timeout=1):
                return
        except OSError:
            time.sleep(0.5)
    raise RuntimeError("community MQTT listener did not start")


def main():
    wait_for_listener()
    with ExitStack() as sockets:
        for number in range(CLIENTS):
            sock = sockets.enter_context(socket.create_connection((HOST, PORT), timeout=5))
            sock.settimeout(5)
            client_id = f"capacity-{number}".encode("ascii")
            body = b"\x00\x04MQTT\x04\x02\x00\x3c" + len(client_id).to_bytes(2, "big") + client_id
            sock.sendall(bytes((0x10, len(body))) + body)
            reply = read_exact(sock, 4)
            if reply != b"\x20\x02\x00\x00":
                raise RuntimeError(f"client {number} received {reply!r} instead of CONNACK")
        print(f"{CLIENTS} MQTT clients connected concurrently")


if __name__ == "__main__":
    main()
