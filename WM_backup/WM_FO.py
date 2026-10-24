"""WM_FO: Field Operator application.

Requests irrigation through Kafka, shows every step CENTRAL reports and lists the
stations. Can also replay a requests file (one service at a time, 4 s between them).
"""

import signal
import sys
import threading
import time

from wm_common import *  # noqa: F401,F403

USAGE_MSG = """
Usage:
    WM_FO <kafka_host>:<kafka_port> <operator_id> [requests_file]

requests_file: one service per line -> "<ws_id>[,<seconds>]"   ('#' starts a comment), e.g.
    # ws_id, seconds
    1,15
    3
"""

MENU = """
Commands:
  l                 list watering stations
  s <ws_id> [secs]  request irrigation (default 20 s)
  f <file>          run the services of a file
  q                 quit
"""

PAUSE_BETWEEN_SERVICES = 4  # seconds, as required by the spec


class FieldOperator:
    def __init__(self, kafka: str, fo_id: str):
        self.bus = Bus(kafka, f"wm-fo-{fo_id}")
        self.fo_id = fo_id
        self.stop = threading.Event()
        self.stations: dict[int, dict] = {}
        self.waiters: dict[str, threading.Event] = {}
        self.ready = None

    def say(self, text: str):
        print(f"[{stamp()}] {text}", flush=True)

    # -- Kafka ------------------------------------------------------------------
    def on_message(self, topic: str, m: dict):
        if topic == T_STATE:
            self.stations[m["ws_id"]] = m
        elif topic == T_FO_NOTIFY and m.get("fo_id") == self.fo_id:
            kind = m.get("kind")
            prefix = {
                "DENIED": "✗",
                "AUTHORIZED": "✓",
                "SUMMARY": "■",
                "PROGRESS": "·",
            }.get(kind, "→")
            self.say(f"{prefix} {m.get('text')}")
            if kind in ("DENIED", "SUMMARY"):
                waiter = self.waiters.get(m.get("req_id"))
                if waiter:
                    waiter.set()

    def request(self, ws_id: int, duration: int = DEFAULT_DURATION) -> str:
        req_id = new_id()
        self.waiters[req_id] = threading.Event()
        self.say(f"Solicitando riego en {ws_name(ws_id)} ({duration} s)...")
        self.bus.publish(
            T_REQUESTS,
            {
                "type": "START_REQ",
                "fo_id": self.fo_id,
                "ws_id": ws_id,
                "req_id": req_id,
                "duration": duration,
            },
            key=self.fo_id,
        )
        return req_id

    def request_and_wait(self, ws_id: int, duration: int = DEFAULT_DURATION):
        req_id = self.request(ws_id, duration)
        if not self.waiters[req_id].wait(duration + 30):
            self.say(f"CENTRAL no ha respondido a la petición sobre {ws_name(ws_id)}")
        self.waiters.pop(req_id, None)

    # -- commands ---------------------------------------------------------------
    def list_stations(self):
        if not self.stations:
            print(
                "(todavía no se ha recibido el estado de CENTRAL, espera unos segundos)"
            )
            return
        print(f"{'ID':<7}{'Ubicación':<24}{'Estado':<20}")
        for ws_id in sorted(self.stations):
            s = self.stations[ws_id]
            extra = (
                f"  {s['flow']} L/min, {s['volume']} L ({s['fo_id']})"
                if s["status"] == WATERING
                else ""
            )
            print(
                f"{ws_name(ws_id):<7}{s['name']:<24}{STATUS_ES[s['status']]:<20}{extra}"
            )

    def run_file(self, path: str):
        try:
            with open(path, encoding="utf-8") as fh:
                lines = [ln.split("#")[0].strip() for ln in fh]
        except OSError as exc:
            self.say(f"No se puede leer '{path}': {exc}")
            return
        services = []
        for number, line in enumerate(lines, 1):
            if not line:
                continue
            try:
                parts = line.replace(",", " ").split()
                services.append(
                    (
                        int(parts[0]),
                        int(parts[1]) if len(parts) > 1 else DEFAULT_DURATION,
                    )
                )
            except ValueError:
                self.say(f"{path}:{number}: línea ignorada '{line}'")
        for index, (ws_id, duration) in enumerate(services):
            if self.stop.is_set():
                return
            self.request_and_wait(ws_id, duration)
            if index < len(services) - 1:
                self.say(
                    f"Esperando {PAUSE_BETWEEN_SERVICES} s antes del siguiente servicio..."
                )
                self.stop.wait(PAUSE_BETWEEN_SERVICES)
        self.say(f"Fichero '{path}' completado ({len(services)} servicios)")

    def menu(self):
        print(MENU)
        while not self.stop.is_set():
            try:
                parts = input("> ").split()
            except EOFError:
                return
            if not parts:
                continue
            key = parts[0].lower()
            try:
                if key == "l":
                    self.list_stations()
                elif key == "s" and len(parts) >= 2:
                    threading.Thread(
                        target=self.request_and_wait,
                        daemon=True,
                        args=(
                            int(parts[1]),
                            int(parts[2]) if len(parts) > 2 else DEFAULT_DURATION,
                        ),
                    ).start()
                elif key == "f" and len(parts) >= 2:
                    threading.Thread(
                        target=self.run_file, args=(parts[1],), daemon=True
                    ).start()
                elif key == "q":
                    self.stop.set()
                else:
                    print(MENU)
            except ValueError:
                print(MENU)

    def run(self, requests_file=None):
        self.bus.ensure_topics()
        self.ready = self.bus.listen(
            [T_FO_NOTIFY, T_STATE], self.on_message, self.stop, f"fo-{self.fo_id}"
        )
        wait_ready(self.ready)
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())
        self.say(f"Operario {self.fo_id} conectado")
        if requests_file:
            self.run_file(requests_file)
            if not sys.stdin.isatty():
                self.stop.set()
        if not self.stop.is_set():
            threading.Thread(target=self.menu, daemon=True).start()
        while not self.stop.wait(1):
            pass
        self.bus.flush()


def main(*args):
    if len(args) not in [2, 3] or args[0].find(":") == -1:
        print(USAGE_MSG)
        sys.exit()
    try:
        host, port = parse_hostport(args[0])
        fo_id = args[1]
    except (IndexError, ValueError):
        print(USAGE_MSG)
        exit(1)
    FieldOperator(f"{host}:{port}", fo_id).run(args[2] if len(args) == 3 else None)


if __name__ == "__main__":
    main(sys.argv[1:])
