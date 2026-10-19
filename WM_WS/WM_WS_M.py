"""Watering Station: WM_WS_M (Monitor) and WM_WS_E (Engine).

Monitor  <--sockets-->  CENTRAL   (registration/authentication + health, STX/ETX/LRC)
Monitor  <--sockets-->  Engine    (ID assignment + HEALTH check every second)
Engine   <--Kafka---->  CENTRAL   (commands, telemetry, events)
"""
import os
import random
import signal
import sys
import threading
import time

from wm_common import *  # noqa: F401,F403

MONITOR_USAGE = '''
Usage:
    WM_WS_M <listen_port> <central_host>:<central_port> <ws_id> [location_name] [x] [y]

    listen_port    port where this Monitor waits for its Engine
    ws_id          unique integer id of this station
Environment: WM_TOKEN (shared secret sent to CENTRAL when registering)
'''

ENGINE_USAGE = '''
Usage:
    WM_WS_E <kafka_host>:<kafka_port> <monitor_host>:<monitor_port>

Keys (followed by Enter):  s [seconds] = request irrigation | f = finish irrigation
                           k = toggle KO (simulated leak)   | t = status | q = quit
'''


# ==========================================================================
# MONITOR
# ==========================================================================
class Monitor:
    def __init__(self, listen_port: int, central: tuple, ws_id: int, name: str, x: float, y: float):
        self.listen_port, self.central = listen_port, central
        self.ws_id, self.name, self.x, self.y = ws_id, name.replace("#", " "), x, y
        self.token = os.environ.get("WM_TOKEN", "")
        self.stop = threading.Event()
        self.authed = threading.Event()   # set while CENTRAL has validated us
        self.health = "WAIT"              # WAIT (no engine yet) -> OK / KO

    def log(self, text: str):
        print(f"[{stamp()}] MONITOR {ws_name(self.ws_id)}: {text}", flush=True)

    # -- link with CENTRAL ---------------------------------------------------
    def central_loop(self):
        while not self.stop.is_set():
            ch = None
            try:
                ch = Channel.connect(*self.central, timeout=5)
                ch.send(f"REG#{self.ws_id}#{self.name}#{self.x}#{self.y}#{self.token}")
                answer = ch.recv()
                if answer != "OK":
                    self.log(f"CENTRAL rechazó el registro: {answer}")
                    self.stop.wait(3)
                    continue
                self.log("autenticada en CENTRAL")
                self.authed.set()
                while not self.stop.is_set():
                    ch.send(f"STATUS#{self.ws_id}#{self.health}")
                    if ch.recv() != "OK":
                        raise ConnectionError("CENTRAL no confirmó el estado")
                    self.stop.wait(1)
            except OSError as exc:
                if self.authed.is_set():
                    self.log(f"enlace con CENTRAL perdido ({exc})")
                else:
                    self.log(f"CENTRAL no disponible ({exc}), reintentando...")
            finally:
                self.authed.clear()
                if ch:
                    ch.close()
            self.stop.wait(2)

    # -- link with the Engine -----------------------------------------------
    def engine_server(self, srv: socket.socket):
        while not self.stop.is_set():
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self.serve_engine(conn)   # one Engine at a time
        srv.close()

    def serve_engine(self, conn: socket.socket):
        ch, engine_up = None, False
        try:
            ch = Channel.accept(conn, timeout=3)
            while not self.authed.wait(1):    # do not assign an id until CENTRAL validated us
                if self.stop.is_set():
                    return
            ch.send(f"ASSIGN#{self.ws_id}")
            if ch.recv() != "OK":
                raise ConnectionError("el engine rechazó el ID")
            engine_up = True
            self.log("engine conectado")
            while not self.stop.is_set():
                ch.send("HEALTH")
                answer = ch.recv()
                new = "OK" if answer == "OK" else "KO"
                if new != self.health:
                    self.log("salud OK" if new == "OK" else "el engine reporta KO -> AVERÍA")
                self.health = new
                self.stop.wait(1)
        except OSError as exc:
            if engine_up:
                self.log(f"el engine no responde ({exc}) -> AVERÍA")
        finally:
            if engine_up:
                self.health = "KO"
            if ch:
                ch.close()
            else:
                conn.close()

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.listen_port))
        srv.listen(4)
        srv.settimeout(1.0)
        threading.Thread(target=self.engine_server, args=(srv,), daemon=True).start()
        threading.Thread(target=self.central_loop, daemon=True).start()
        self.log(f"en marcha, esperando al engine en :{self.listen_port}")
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())
        while not self.stop.wait(1):
            pass


def monitor(*args: str):
    try:
        port = int(args[0])
        central = parse_hostport(args[1])
        ws_id = int(args[2])
        name = args[3] if len(args) > 3 else f"Zona {ws_id}"
        x = float(args[4]) if len(args) > 4 else 0.0
        y = float(args[5]) if len(args) > 5 else 0.0
    except (IndexError, ValueError):
        print(MONITOR_USAGE)
        return 1
    Monitor(port, central, ws_id, name, x, y).run()


# ==========================================================================
# ENGINE
# ==========================================================================
class Engine:
    def __init__(self, kafka: str, monitor_addr: tuple):
        self.bus = Bus(kafka, "wm-ws-engine")
        self.monitor_addr = monitor_addr
        self.stop = threading.Event()
        self.lock = threading.RLock()
        self.ws_id = None
        self.linked = threading.Event()   # set while the Monitor link is up
        self.ko = False                   # simulated leak
        self.blocked = False              # Fuera de Servicio, ordered by CENTRAL
        self.watering = False
        self.req_id = self.fo_id = None
        self.duration = 0
        self.t0 = 0.0
        self.flow = self.volume = 0.0

    def say(self, text: str):
        print(f"[{stamp()}] ENGINE {ws_name(self.ws_id) if self.ws_id is not None else '(sin ID)'}: {text}", flush=True)

    def status(self) -> str:
        if not self.linked.is_set():
            return DISCONNECTED
        if self.ko:
            return LEAK
        if self.blocked:
            return OUT_OF_SERVICE
        if self.watering:
            return WATERING
        return AVAILABLE

    def event(self, kind: str, **fields):
        self.bus.publish(T_WS_EVENTS, {"type": kind, "ws_id": self.ws_id, **fields}, key=self.ws_id)

    # -- Monitor link ---------------------------------------------------------
    def link_loop(self):
        while not self.stop.is_set():
            ch = None
            try:
                ch = Channel.connect(*self.monitor_addr, timeout=20)  # the Monitor waits for CENTRAL first
                message = ch.recv()
                if not message or not message.startswith("ASSIGN#"):
                    raise ConnectionError("asignación de ID no válida")
                self.ws_id = int(message.split("#")[1])
                ch.send("OK")
                ch.set_timeout(3)
                self.linked.set()
                self.say("ID asignado y validado por CENTRAL, enlace con el monitor activo")
                self.event("HELLO")
                while not self.stop.is_set():
                    message = ch.recv()
                    if message is None:
                        raise ConnectionError("el monitor cerró la conexión")
                    if message == "HEALTH":
                        ch.send("KO" if self.ko else "OK")
            except (OSError, ValueError) as exc:
                if self.linked.is_set():
                    self.say(f"enlace con el monitor perdido ({exc})")
                else:
                    self.say(f"monitor no disponible ({exc}), reintentando...")
            finally:
                was_linked = self.linked.is_set()
                self.linked.clear()
                if was_linked:
                    self.finish("DISCONNECTED")
                if ch:
                    ch.close()
            self.stop.wait(2)

    # -- commands from CENTRAL ------------------------------------------------
    def on_cmd(self, topic: str, m: dict):
        if self.ws_id is None or m.get("ws_id") != self.ws_id:
            return
        kind = m.get("type")
        if kind == "START":
            self.start(m)
        elif kind == "STOP":
            self.finish(m.get("reason", "STOPPED"))
        elif kind == "BLOCK":
            if not self.blocked:
                self.say("CENTRAL ordena: FUERA DE SERVICIO")
            self.blocked = True
            self.finish("BLOCKED")
        elif kind == "ACTIVATE":
            if self.blocked:
                self.say("CENTRAL ordena: ACTIVAR (Disponible)")
            self.blocked = False
        elif kind == "INFO":
            self.say(f"CENTRAL: {m.get('text')}")

    def start(self, m: dict):
        with self.lock:
            reason = None
            if not self.linked.is_set():
                reason = "sin enlace con el monitor"
            elif self.ko:
                reason = "avería/fuga en la estación"
            elif self.blocked:
                reason = "estación Fuera de Servicio"
            elif self.watering:
                reason = "la estación ya está regando"
            if reason is None:
                self.watering = True
                self.req_id, self.fo_id = m.get("req_id"), m.get("fo_id")
                self.duration = int(m.get("duration") or DEFAULT_DURATION)
                self.t0 = time.monotonic()
                self.flow = self.volume = 0.0
        if reason:
            self.say(f"riego rechazado: {reason}")
            self.event("REJECTED", req_id=m.get("req_id"), reason=reason)
            return
        self.say(f"electroválvula ABIERTA (operario {self.fo_id}, límite {self.duration} s)")
        self.event("STARTED", req_id=m.get("req_id"), fo_id=self.fo_id)

    def finish(self, reason: str):
        with self.lock:
            if not self.watering:
                return
            self.watering = False
            req_id, fo_id = self.req_id, self.fo_id
            volume, duration = self.volume, int(time.monotonic() - self.t0)
            self.flow = 0.0
        self.say(f"electroválvula CERRADA ({reason}). Volumen {volume:.1f} L en {duration} s")
        self.event("FINISHED", req_id=req_id, fo_id=fo_id, reason=reason,
                   volume=round(volume, 2), duration=duration)

    def ticker(self):
        """Simulated flow meter: every second while the valve is open."""
        while not self.stop.wait(1.0):
            with self.lock:
                if not self.watering:
                    continue
                elapsed = int(time.monotonic() - self.t0)
                self.flow = round(random.uniform(9.0, 12.0), 1)
                self.volume += self.flow / 60.0
                payload = dict(req_id=self.req_id, flow=self.flow, volume=round(self.volume, 2), elapsed=elapsed)
                done = elapsed >= self.duration
                line = (f"REGANDO | caudal {self.flow:.1f} L/min | volumen {self.volume:.1f} L | "
                        f"{elapsed}/{self.duration} s | operario {self.fo_id}")
            self.event("TELEMETRY", **payload)
            self.say(line)
            if done:
                self.finish("TIMEOUT")

    # -- local menu -----------------------------------------------------------
    def menu(self):
        print(ENGINE_USAGE, flush=True)
        for line in sys.stdin:
            parts = line.split()
            if not parts:
                continue
            key = parts[0].lower()
            if key == "s":
                if self.ws_id is None:
                    self.say("todavía no tengo ID asignado")
                    continue
                try:
                    duration = int(parts[1]) if len(parts) > 1 else DEFAULT_DURATION
                except ValueError:
                    duration = DEFAULT_DURATION
                self.say(f"solicitando riego a CENTRAL ({duration} s)")
                self.bus.publish(T_REQUESTS, {"type": "START_REQ", "fo_id": "LOCAL", "ws_id": self.ws_id,
                                              "req_id": new_id(), "duration": duration}, key=self.ws_id)
            elif key == "f":
                self.finish("MANUAL")
            elif key == "k":
                self.ko = not self.ko
                self.say("KO activado: simulando FUGA" if self.ko else "KO desactivado: avería resuelta")
                if self.ko:
                    self.finish("LEAK")
            elif key == "t":
                self.say(f"estado {STATUS_ES[self.status()]}")
            elif key == "q":
                self.stop.set()
                return
            else:
                print(ENGINE_USAGE, flush=True)

    def run(self):
        self.bus.ensure_topics()
        ready = self.bus.listen([T_WS_CMD], self.on_cmd, self.stop, "ws-engine")
        wait_ready(ready)
        threading.Thread(target=self.link_loop, daemon=True).start()
        threading.Thread(target=self.ticker, daemon=True).start()
        threading.Thread(target=self.menu, daemon=True).start()
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())
        while not self.stop.wait(1):
            pass
        self.finish("STOPPED")
        self.bus.flush()


def engine(*args: str):
    try:
        kafka = parse_hostport(args[0])
        monitor_addr = parse_hostport(args[1])
    except (IndexError, ValueError):
        print(ENGINE_USAGE)
        return 1
    Engine(f"{kafka[0]}:{kafka[1]}", monitor_addr).run()
