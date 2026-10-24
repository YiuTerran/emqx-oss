#!/usr/bin/env python3
"""Verify the release image in the isolated three-core Compose cluster."""

import argparse
import json
import os
import re
import socket
import struct
import subprocess
import time
import uuid
from pathlib import Path
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "deploy/docker/compose.cluster.yaml"
NODES = ("emqx1", "emqx2", "emqx3")
MQTT_PORTS = (21881, 21882, 21883)
DASHBOARD_PORTS = (28081, 28082, 28083)


def compose(project, *args, timeout=60):
    result = subprocess.run(
        ["docker", "compose", "-p", project, "-f", str(COMPOSE_FILE), *args],
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if result.returncode:
        raise RuntimeError(f"compose {' '.join(args)}: {result.stdout}\n{result.stderr}")
    return result.stdout


def wait_for_cluster(project):
    expected = "['emqx@emqx1.emqx.local','emqx@emqx2.emqx.local','emqx@emqx3.emqx.local']"
    expression = (
        "mnesia_hook = mnesia_hook:module_info(module), "
        "rlog = mria_config:backend(), core = mria_config:role(), "
        f"{expected} = lists:sort(mria:running_nodes()), "
        'io:format("CLUSTER_OK ~s~n", [emqx_release:version()]).'
    )
    deadline = time.monotonic() + 180
    last_error = None
    while time.monotonic() < deadline:
        try:
            for node in NODES:
                output = compose(project, "exec", "-T", node, "emqx", "eval", expression)
                if "CLUSTER_OK" not in output:
                    raise RuntimeError(f"{node}: unexpected runtime response: {output}")
            print("PASS all three nodes: custom OTP, RLOG, core role, complete cluster", flush=True)
            return
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            last_error = error
            time.sleep(3)
    raise RuntimeError(f"cluster did not converge: {last_error}")


def api(port, path, data=None, token=None):
    headers = {}
    if data is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(
        f"http://127.0.0.1:{port}{path}",
        data=None if data is None else json.dumps(data).encode(),
        headers=headers,
    )
    with urlopen(request, timeout=15) as response:
        return response.read()


def check_dashboard():
    expected = re.search(
        r"^export EMQX_DASHBOARD_VERSION \?= (\S+)$",
        (ROOT / "Makefile").read_text(),
        re.MULTILINE,
    ).group(1)
    password = os.environ.get("EMQX_DASHBOARD_PASSWORD", "public")
    for port in DASHBOARD_PORTS:
        index = api(port, "/").decode()
        if '<div id="app">' not in index:
            raise RuntimeError(f"dashboard {port}: index is missing the Vue app")
        version = api(port, "/version").decode()
        if f"github_ref: refs/tags/{expected}\n" not in version:
            raise RuntimeError(f"dashboard {port}: unexpected build metadata: {version}")
        scripts = re.findall(r'<script[^>]+src="([^"]*static/js/app[^"]+)"', index)
        if not scripts:
            raise RuntimeError(f"dashboard {port}: app bundle missing from HTML")
        bundle = api(port, "/" + scripts[0].removeprefix("./").lstrip("/")).decode()
        if "Kafka Producer" not in bundle or "kafka_producer" not in bundle:
            raise RuntimeError(f"dashboard {port}: custom Kafka UI missing")
        login = json.loads(api(port, "/api/v5/login", {"username": "admin", "password": password}))
        nodes = json.loads(api(port, "/api/v5/nodes", token=login["token"]))
        if {node["node"] for node in nodes} != {f"emqx@{name}.emqx.local" for name in NODES}:
            raise RuntimeError(f"dashboard {port}: API does not report all three nodes: {nodes}")
    print(f"PASS all three dashboards: {expected}, Kafka bundle, login and nodes API", flush=True)


def mqtt_string(value):
    value = value.encode() if isinstance(value, str) else value
    return struct.pack("!H", len(value)) + value


def read_exact(sock, size):
    result = bytearray()
    while len(result) < size:
        part = sock.recv(size - len(result))
        if not part:
            raise RuntimeError("MQTT peer closed the connection")
        result.extend(part)
    return bytes(result)


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.sock.settimeout(10)
        client_id = "cluster-it-" + uuid.uuid4().hex
        self.send(0x10, b"\x00\x04MQTT\x04\x02\x00\x3c" + mqtt_string(client_id))
        if self.receive() != (0x20, b"\x00\x00"):
            raise RuntimeError("MQTT CONNECT was rejected")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.sock.close()

    def send(self, header, body):
        remaining = len(body)
        length = bytearray()
        while True:
            digit = remaining % 128
            remaining //= 128
            length.append(digit | (0x80 if remaining else 0))
            if not remaining:
                break
        self.sock.sendall(bytes([header]) + length + body)

    def receive(self):
        header = read_exact(self.sock, 1)[0]
        size = 0
        for shift in range(0, 28, 7):
            digit = read_exact(self.sock, 1)[0]
            size += (digit & 0x7F) << shift
            if digit < 128:
                return header, read_exact(self.sock, size)
        raise RuntimeError("malformed MQTT remaining length")

    def expect(self, header, body):
        packet = self.receive()
        if packet != (header, body):
            raise RuntimeError(f"expected {(header, body)!r}, received {packet!r}")

    def subscribe(self, topic):
        self.send(0x82, b"\x00\x01" + mqtt_string(topic) + b"\x02")
        self.expect(0x90, b"\x00\x01\x02")

    def publish(self, topic, payload, qos, retain=False):
        body = mqtt_string(topic)
        if qos:
            body += b"\x00\x02"
        self.send(0x30 | (qos << 1) | int(retain), body + payload)
        if qos == 1:
            self.expect(0x40, b"\x00\x02")
        elif qos == 2:
            self.expect(0x50, b"\x00\x02")
            self.send(0x62, b"\x00\x02")
            self.expect(0x70, b"\x00\x02")

    def message(self, topic, payload, qos, retained=False):
        header, body = self.receive()
        if header >> 4 != 3 or (header >> 1) & 3 != qos or bool(header & 1) != retained:
            raise RuntimeError(f"unexpected PUBLISH header: {header:#x}")
        topic_length = struct.unpack("!H", body[:2])[0]
        received_topic = body[2:2 + topic_length].decode()
        offset = 2 + topic_length
        packet_id = body[offset:offset + 2] if qos else b""
        offset += 2 if qos else 0
        if received_topic != topic or body[offset:] != payload:
            raise RuntimeError(f"unexpected MQTT message: {received_topic}, {body[offset:]!r}")
        if qos == 1:
            self.send(0x40, packet_id)
        elif qos == 2:
            self.send(0x50, packet_id)
            self.expect(0x62, packet_id)
            self.send(0x70, packet_id)


def check_messages(prefix, members=(0, 1, 2)):
    count = 0
    for source in members:
        for target in members:
            if source == target:
                continue
            for qos in range(3):
                topic = f"{prefix}/from-{source}/to-{target}/qos-{qos}"
                payload = uuid.uuid4().hex.encode()
                with Client(MQTT_PORTS[target]) as subscriber, Client(MQTT_PORTS[source]) as publisher:
                    subscriber.subscribe(topic)
                    # Allow the route to propagate before publishing at QoS 0.
                    time.sleep(0.2)
                    publisher.publish(topic, payload, qos)
                    subscriber.message(topic, payload, qos)
                count += 1
    print(f"PASS {count} cross-node MQTT deliveries: all node pairs, QoS 0/1/2", flush=True)


def check_retained(topic, payload):
    for port in MQTT_PORTS:
        with Client(port) as subscriber:
            subscriber.subscribe(topic)
            subscriber.message(topic, payload, 1, retained=True)
    print("PASS retained message visible from all three nodes", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="emqx-cluster-it")
    parser.add_argument("--restart-node", action="store_true", help="restart emqx2 and repeat checks")
    args = parser.parse_args()
    wait_for_cluster(args.project)
    check_dashboard()
    prefix = "cluster-it/" + uuid.uuid4().hex
    retained_topic = prefix + "/retained"
    payload = uuid.uuid4().hex.encode()
    with Client(MQTT_PORTS[0]) as publisher:
        publisher.publish(retained_topic, payload, 1, retain=True)
    try:
        check_messages(prefix + "/before")
        check_retained(retained_topic, payload)
        if args.restart_node:
            compose(args.project, "stop", "emqx2", timeout=90)
            try:
                check_messages(prefix + "/offline", members=(0, 2))
                print("PASS remaining nodes deliver messages while emqx2 is stopped", flush=True)
            finally:
                compose(args.project, "start", "emqx2")
            wait_for_cluster(args.project)
            check_dashboard()
            check_messages(prefix + "/after")
            check_retained(retained_topic, payload)
            print("PASS node restart, cluster rejoin and message recovery", flush=True)
    finally:
        with Client(MQTT_PORTS[0]) as publisher:
            publisher.publish(retained_topic, b"", 1, retain=True)
    print("PASS three-node release integration test", flush=True)


if __name__ == "__main__":
    main()
