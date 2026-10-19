"""Pieces shared by every WM module.

- Kafka topics and message conventions
- <STX><DATA><ETX><LRC> socket framing (with ENQ/ACK/NACK/EOT handshake)
- A small Kafka wrapper (JSON producer + background JSON consumers)
"""

import json
import socket
import sys
import threading
import time
import traceback
import uuid

from typing import Callable
from concurrent.futures import Future

from confluent_kafka import \
    Consumer, KafkaError, KafkaException, Producer, Message
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

# --------------------------------------------------------------------------
# Domain constants
# --------------------------------------------------------------------------
AVAILABLE = "AVAILABLE"
WATERING = "WATERING"
LEAK = "LEAK"
OUT_OF_SERVICE = "OUT_OF_SERVICE"
DISCONNECTED = "DISCONNECTED"

STATUS_ES = {
    AVAILABLE: "Disponible",
    WATERING: "Regando",
    LEAK: "Fuga",
    OUT_OF_SERVICE: "Fuera de Servicio",
    DISCONNECTED: "Desconectada",
}

# Kafka topics (single partition each, so ordering is guaranteed)
T_REQUESTS = "wm.requests"    # FO / WS menu  -> CENTRAL   (START_REQ)
T_WS_CMD = "wm.ws.cmd"        # CENTRAL       -> Engine    (START, STOP, BLOCK, ACTIVATE, INFO)
T_WS_EVENTS = "wm.ws.events"  # Engine        -> CENTRAL   (HELLO, STARTED, REJECTED, TELEMETRY, FINISHED)
T_FO_NOTIFY = "wm.fo.notify"  # CENTRAL       -> FO        (INFO, AUTHORIZED, DENIED, PROGRESS, SUMMARY)
T_STATE = "wm.state"          # CENTRAL       -> everybody (STATE of every WS)
TOPICS = [T_REQUESTS, T_WS_CMD, T_WS_EVENTS, T_FO_NOTIFY, T_STATE]

DEFAULT_DURATION = 20  # seconds, when a request does not specify one


def ws_name(ws_id: int) -> str:
    return f"WS-{int(ws_id):02d}"


def stamp() -> str:
    return time.strftime("%H:%M:%S")


def new_id() -> str:
    return uuid.uuid4().hex[:8]


def parse_hostport(text: str, default_host: str = "localhost") -> tuple[str, int]:
    """'host:port' or just 'port' -> (host, port)."""
    host, _, port = text.rpartition(":")
    if not port.isdigit():
        raise ValueError(f"'{text}' is not a valid [host:]port")
    return (host or default_host, int(port))


# --------------------------------------------------------------------------
# Socket protocol:  <ENQ> -> <ACK> , then (<STX>DATA<ETX><LRC> -> <ACK>/<NACK>)*, then <EOT>
# LRC = XOR of every DATA byte. Fields inside DATA are separated by '#'.
# --------------------------------------------------------------------------
STX, ETX, ENQ, ACK, NACK, EOT = b"\x02", b"\x03", b"\x05", b"\x06", b"\x15", b"\x04"


def lrc(data: bytes) -> int:
    result = 0
    for byte in data:
        result ^= byte
    return result


class Channel:
    """One framed, acknowledged conversation over a connected socket.

    send() and recv() raise OSError (ConnectionError / TimeoutError included)
    when the peer disappears, so callers only need `except OSError`.
    """

    def __init__(self, sock: socket.socket, timeout: float = 5.0):
        self.sock: socket.socket = sock
        self.sock.settimeout(timeout)
        try:
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        self._buf: bytes = b""

    # -- construction -------------------------------------------------------
    @classmethod
    def connect(cls, host: str, port: int, timeout: float = 5.0) -> "Channel":
        """Client side: connect + ENQ/ACK handshake."""
        sock = socket.create_connection((host, port), timeout=timeout)
        ch = cls(sock, timeout)
        try:
            sock.sendall(ENQ)
            if ch._read(1) != ACK:
                raise ConnectionError("handshake refused")
        except OSError:
            sock.close()
            raise
        return ch

    @classmethod
    def accept(cls, sock: socket.socket, timeout: float = 5.0) -> "Channel":
        """Server side: wait for ENQ and answer ACK."""
        ch = cls(sock, timeout)
        if ch._read(1) != ENQ:
            sock.sendall(NACK)
            raise ConnectionError("expected ENQ")
        sock.sendall(ACK)
        return ch

    def set_timeout(self, timeout: float):
        self.sock.settimeout(timeout)

    # -- raw reading --------------------------------------------------------
    def _read(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _read_until(self, delimiter: bytes) -> bytes:
        while delimiter not in self._buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed")
            self._buf += chunk
        data, _, self._buf = self._buf.partition(delimiter)
        return data

    # -- messages -----------------------------------------------------------
    def send(self, message: str, retries: int = 3):
        data = message.encode("utf-8")
        frame = STX + data + ETX + bytes([lrc(data)])
        for _ in range(retries):
            self.sock.sendall(frame)
            if self._read(1) == ACK:
                return
        raise ConnectionError("peer kept answering NACK")

    def recv(self):
        """Next message as str, or None if the peer sent <EOT>."""
        while True:
            first = self._read(1)
            if first == EOT:
                return None
            if first != STX:
                continue  # noise
            data = self._read_until(ETX)
            check = self._read(1)[0]
            if check == lrc(data):
                self.sock.sendall(ACK)
                return data.decode("utf-8", errors="replace")
            self.sock.sendall(NACK)  # bad LRC: peer will retransmit

    def close(self):
        try:
            self.sock.sendall(EOT)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


# --------------------------------------------------------------------------
# Kafka
# --------------------------------------------------------------------------
def _on_delivery(err: KafkaError | None, msg: Message):
    if err is not None:
        print(f"[kafka] delivery failed on {msg.topic()}: {err}", file=sys.stderr, flush=True)


class Bus:
    """JSON producer + JSON consumers over Kafka."""

    def __init__(self, bootstrap: str, client: str = "wm"):
        self.bootstrap: str = bootstrap
        self.producer: Producer = Producer({"bootstrap.servers": bootstrap, "client.id": client})

    def publish(self, topic: str, payload: dict[object, object], key: str | None =None):
        data = json.dumps(payload).encode("utf-8")
        key_bytes = None if key is None else str(key).encode("utf-8")
        while True:
            try:
                self.producer.produce(
                    topic, data, key=key_bytes, on_delivery=_on_delivery
                )
                break
            except BufferError:
                _ = self.producer.poll(0.1)
        _ = self.producer.poll(0)

    def flush(self, timeout: float = 5.0):
        _ = self.producer.flush(timeout)

    def ensure_topics(self):
        """Create the topics if missing (brokers may have auto-create disabled)."""
        try:
            admin = AdminClient({"bootstrap.servers": self.bootstrap})
            futures: dict[str, Future[None]] = admin.create_topics(
                [NewTopic(t, num_partitions=1, replication_factor=1) for t in TOPICS],
                request_timeout=10,
            )
            for topic, future in futures.items():
                try:
                    future.result()
                except KafkaException as exc:
                    if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                        print(f"[kafka] could not create {topic}: {exc}", file=sys.stderr, flush=True)
        except Exception as exc:  # broker unreachable, etc.
            print(f"[kafka] ensure_topics failed: {exc}", file=sys.stderr, flush=True)

    def listen(
        self, topics: list[str], handler: Callable[str, dict],
            stop: threading.Event, role: str = "wm"
    ) -> threading.Event:
        """Start a daemon thread that calls handler(topic, payload_dict) for every message.

        Every listener gets its own consumer group and starts at the END of the
        topics, so it only sees what happens while it is running (no stale
        commands replayed after a restart).  Returns an Event that is set once
        the partitions are assigned (see wait_ready).
        """
        ready = threading.Event()
        consumer = Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": f"wm-{role}-{new_id()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        consumer.subscribe(list(topics), on_assign=lambda c, parts: ready.set())

        def loop():
            try:
                while not stop.is_set():
                    msg = consumer.poll(0.5)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            print(f"[kafka] {msg.error()}", file=sys.stderr, flush=True)
                        continue
                    try:
                        payload = json.loads(msg.value())
                    except (ValueError, TypeError):
                        continue
                    if not isinstance(payload, dict):
                        continue
                    try:
                        handler(msg.topic(), payload)
                    except Exception:
                        traceback.print_exc()
            finally:
                consumer.close()

        threading.Thread(target=loop, daemon=True, name=f"kafka-{role}").start()
        return ready


def wait_ready(ready: threading.Event, timeout: float = 20.0) -> bool:
    """Block until a listener has its partitions (plus a short grace period)."""
    ok = ready.wait(timeout)
    time.sleep(0.5)
    return ok
