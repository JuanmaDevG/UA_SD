"""WM_Central: system logic of WaterManagement.

 - Socket server (STX/ETX/LRC) where every WS Monitor registers/authenticates and sends its health.
 - Kafka: receives irrigation requests + Engine events, sends commands, notifications and state.
 - SQLite: stations and operators.
 - Web panel (stdlib http.server): live monitoring + Start / Block / Activate controls.
"""
import os
import signal
import sqlite3
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import random

from wm_common import *

USAGE_MSG = '''
Usage:
    WM_Central <listen_port> <kafka_host>:<kafka_port>

Both can come from the environment instead: WM_PORT, WM_KAFKA.
Other environment variables:
    WM_HTTP_PORT     web panel port (default: $PORT or 8080)
    WM_VOLUME        folder where wm.db lives (default: .)
    WM_TOKEN         shared secret every WM_WS_M must send when registering (optional)
    WM_ADMIN_TOKEN   token required by the panel to send commands (optional)
    WM_OPERATORS     extra operator ids, comma separated (FO-01..FO-05 always exist)
'''

DB_SCHEMA = '''
CREATE TABLE IF NOT EXISTS ws (
    id INTEGER PRIMARY KEY,
    x REAL, y REAL,
    name TEXT,
    blocked INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS operators (
    id TEXT PRIMARY KEY,
    name TEXT
);
'''

LINK_TIMEOUT = 5.0     # seconds without a STATUS from a Monitor -> DISCONNECTED
START_TIMEOUT = 8.0    # seconds an Engine has to answer a START command
SNAPSHOT_EVERY = 5.0   # seconds between full state broadcasts (for late joiners)

REASON_ES = {
    "TIMEOUT": "tiempo límite alcanzado",
    "MANUAL": "finalizado desde la estación",
    "LEAK": "fuga detectada",
    "BLOCKED": "estación puesta Fuera de Servicio por CENTRAL",
    "DISCONNECTED": "estación desconectada",
    "STOPPED": "estación detenida",
}


def status_of(st: dict[object, object]) -> str:
    if not st["connected"] or st["health"] == "WAIT":
        return DISCONNECTED
    if st["health"] == "KO":
        return LEAK
    if st["blocked"]:
        return OUT_OF_SERVICE
    if st["watering"]:
        return WATERING
    return AVAILABLE


class Central:
    def __init__(self, port: int, bootstrap: str):
        self.port: int = port
        self.bootstrap: str = bootstrap
        self.bus: Bus = Bus(bootstrap, "wm-central")
        self.stop: threading.Event = threading.Event()
        self.lock: threading.RLock = threading.RLock()       # protects self.stations
        self.dblock: threading.Lock = threading.Lock()      # protects the sqlite connection
        self.stations: dict[int, dict[object, object]] = {}
        self.messages: deque[object] = deque(maxlen=200)
        self.started: float = time.time()
        self.srv: socket.socket | None = None
        self.db: sqlite3.Connection

    # ------------------------------------------------------------------ logging
    def log(self, text: str):
        line = f"[{stamp()}] {text}"
        print(line, flush=True)
        self.messages.append(line)

    # ------------------------------------------------------------------ database
    def db_init(self):
        folder = os.environ.get("WM_VOLUME", ".")
        os.makedirs(folder, exist_ok=True)
        self.db = sqlite3.connect(os.path.join(folder, "wm.db"), check_same_thread=False)
        _ = self.db.executescript(DB_SCHEMA)
        extra = [o.strip() for o in os.environ.get("WM_OPERATORS", "").split(",") if o.strip()]
        for op in [f"FO-{i:02d}" for i in range(1, 6)] + extra:
            _ = self.db.execute(
                "INSERT OR IGNORE INTO operators(id, name) VALUES (?, ?)",
                (op, op, random.uniform(0, 1), random.uniform(0, 1))
            )
        self.db.commit()

    def sql(self, query: str, args: tuple[object, ...] = ()):
        with self.dblock:
            rows = self.db.execute(query, args).fetchall()
            self.db.commit()
            return rows

    def load_stations(self):
        """Step 1 of the spec: known stations are shown as DISCONNECTED until they connect."""
        for ws_id, x, y, name, blocked in self.sql("SELECT id, x, y, name, blocked FROM ws"):
            self.stations[ws_id] = self.new_station(ws_id, name, x, y, blocked)
        self.log(f"CENTRAL: {len(self.stations)} estaciones cargadas de la BD (Desconectadas hasta que conecten)")

    @staticmethod
    def new_station(ws_id, name, x, y, blocked) -> dict:
        return dict(id=ws_id, name=name, x=x, y=y, blocked=bool(blocked),
                    connected=False, link=None, health="WAIT", pending=None,
                    watering=False, fo_id=None, req_id=None, flow=0.0, volume=0.0,
                    elapsed=0, duration=0, last_watering=None)

    # ------------------------------------------------------------------ state publishing
    @staticmethod
    def snapshot(st: dict) -> dict:
        return {"ws_id": st["id"], "name": st["name"], "x": st["x"], "y": st["y"],
                "status": status_of(st), "flow": round(st["flow"], 1),
                "volume": round(st["volume"], 1), "fo_id": st["fo_id"],
                "elapsed": st["elapsed"], "duration": st["duration"],
                "last_watering": st["last_watering"]}

    def publish_state(self, st: dict):
        with self.lock:
            snap = self.snapshot(st)
        self.bus.publish(T_STATE, {"type": "STATE", **snap}, key=snap["ws_id"])

    def publish_all(self):
        with self.lock:
            snaps = [self.snapshot(s) for s in self.stations.values()]
        for snap in snaps:
            self.bus.publish(T_STATE, {"type": "STATE", **snap}, key=snap["ws_id"])

    def panel_state(self) -> dict:
        with self.lock:
            stations = sorted((self.snapshot(s) for s in self.stations.values()), key=lambda s: s["ws_id"])
        counts = {s: 0 for s in STATUS_ES}
        for s in stations:
            counts[s["status"]] += 1
        return {"stations": stations, "counts": counts, "messages": list(self.messages),
                "uptime": int(time.time() - self.started), "kafka": self.bootstrap, "port": self.port}

    # ------------------------------------------------------------------ requester notifications
    def tell(self, req: dict, kind: str, text: str, quiet: bool = False, **extra):
        """Send a message to whoever asked: an operator (Kafka), a station menu (LOCAL) or the panel (CENTRAL)."""
        fo, ws = req.get("fo_id"), req.get("ws_id")
        if not quiet:
            self.log(f"{ws_name(ws) if ws is not None else 'WS-??'} [{fo}] {text}")
        extra.update(req_id=req.get("req_id"), ws_id=ws)
        if fo == "LOCAL":
            if kind != "PROGRESS":
                self.bus.publish(T_WS_CMD, {"type": "INFO", "kind": kind, "text": text, **extra}, key=ws)
        elif fo != "CENTRAL" and fo is not None:
            self.bus.publish(T_FO_NOTIFY, {"fo_id": fo, "kind": kind, "text": text, **extra}, key=fo)

    # ------------------------------------------------------------------ irrigation requests
    def handle_start(self, req: dict):
        fo = req.get("fo_id")
        try:
            ws_id = int(req.get("ws_id"))
            duration = int(req.get("duration") or DEFAULT_DURATION)
        except (TypeError, ValueError):
            return self.tell(req, "DENIED", "Petición mal formada")
        req["ws_id"] = ws_id
        req["duration"] = duration = max(1, min(duration, 3600))
        req.setdefault("req_id", new_id())

        self.tell(req, "INFO", f"Petición de riego recibida para {ws_name(ws_id)} ({duration} s)")
        if fo not in ("LOCAL", "CENTRAL") and not self.sql("SELECT 1 FROM operators WHERE id = ?", (fo,)):
            return self.tell(req, "DENIED", f"Riego DENEGADO: operario '{fo}' no registrado")

        with self.lock:
            st = self.stations.get(ws_id)
            if st is None:
                return self.tell(req, "DENIED", f"Riego DENEGADO: {ws_name(ws_id)} no existe")
            self.tell(req, "INFO", f"Comprobando estado de {ws_name(ws_id)}...")
            status = status_of(st)
            if status != AVAILABLE:
                return self.tell(req, "DENIED", f"Riego DENEGADO: {ws_name(ws_id)} está {STATUS_ES[status]}")
            if st["pending"]:
                return self.tell(req, "DENIED", f"Riego DENEGADO: {ws_name(ws_id)} está atendiendo otra petición")
            st["pending"] = {"req": req, "deadline": time.time() + START_TIMEOUT}

        self.tell(req, "INFO", f"{ws_name(ws_id)} disponible. Solicitando a la estación que abra la electroválvula...")
        self.bus.publish(T_WS_CMD, {"type": "START", "ws_id": ws_id, "req_id": req["req_id"],
                                    "fo_id": fo, "duration": duration}, key=ws_id)

    def on_started(self, st: dict, m: dict):
        with self.lock:
            pending = st["pending"]
            if not pending or pending["req"]["req_id"] != m.get("req_id"):
                return
            req = pending["req"]
            st.update(pending=None, watering=True, fo_id=req["fo_id"], req_id=req["req_id"],
                      flow=0.0, volume=0.0, elapsed=0, duration=req["duration"])
        self.tell(req, "AUTHORIZED", f"Riego AUTORIZADO en {ws_name(st['id'])}: electroválvula abierta (límite {req['duration']} s)")
        self.publish_state(st)

    def on_rejected(self, st: dict, m: dict):
        with self.lock:
            pending = st["pending"]
            if not pending or pending["req"]["req_id"] != m.get("req_id"):
                return
            st["pending"] = None
        self.tell(pending["req"], "DENIED", f"Riego DENEGADO por la estación: {m.get('reason', 'sin motivo')}")

    def on_telemetry(self, st: dict, m: dict):
        with self.lock:
            if not st["watering"] or st["req_id"] != m.get("req_id"):
                return
            st.update(flow=float(m.get("flow", 0)), volume=float(m.get("volume", 0)), elapsed=int(m.get("elapsed", 0)))
            req = {"fo_id": st["fo_id"], "ws_id": st["id"], "req_id": st["req_id"]}
            text = (f"{ws_name(st['id'])}: caudal {st['flow']:.1f} L/min | volumen {st['volume']:.1f} L "
                    f"| {st['elapsed']}/{st['duration']} s")
        self.tell(req, "PROGRESS", text, quiet=True)
        self.publish_state(st)

    def finish_session(self, st: dict, reason: str, volume=None, duration=None):
        """Close the current irrigation (idempotent) and send the final summary."""
        with self.lock:
            if not st["watering"]:
                return
            req = {"fo_id": st["fo_id"], "ws_id": st["id"], "req_id": st["req_id"]}
            volume = st["volume"] if volume is None else float(volume)
            duration = st["elapsed"] if duration is None else int(duration)
            st.update(watering=False, flow=0.0, fo_id=None, last_watering=time.time())
        self.tell(req, "SUMMARY",
                  f"Riego FINALIZADO en {ws_name(st['id'])} ({REASON_ES.get(reason, reason)}). "
                  f"Volumen total: {volume:.1f} L, duración: {duration} s",
                  reason=reason, volume=volume, duration=duration)
        self.publish_state(st)

    # ------------------------------------------------------------------ health from the Monitor
    def set_health(self, st: dict, health: str):
        with self.lock:
            old = st["health"]
            if old == health:
                return
            st["health"] = health
            pending = st["pending"] if health == "KO" else None
            if pending:
                st["pending"] = None
        name = ws_name(st["id"])
        if health == "KO":
            self.log(f"{name}: AVERÍA/FUGA detectada")
            if st["watering"]:
                self.bus.publish(T_WS_CMD, {"type": "STOP", "ws_id": st["id"], "reason": "LEAK"}, key=st["id"])
                self.finish_session(st, "LEAK")
            if pending:
                self.tell(pending["req"], "DENIED", f"Riego DENEGADO: fuga en {name}")
        elif old == "KO":
            self.log(f"{name}: avería resuelta, vuelve a estar operativa")
        elif old == "WAIT":
            self.log(f"{name}: estación completa (monitor + engine) y operativa")
        self.publish_state(st)

    def sync_engine(self, st: dict):
        """An Engine says HELLO: tell it whether CENTRAL has it blocked."""
        kind = "BLOCK" if st["blocked"] else "ACTIVATE"
        self.bus.publish(T_WS_CMD, {"type": kind, "ws_id": st["id"]}, key=st["id"])

    # ------------------------------------------------------------------ Kafka consumer
    def on_kafka(self, topic: str, m: dict):
        kind = m.get("type")
        if topic == T_REQUESTS and kind == "START_REQ":
            return self.handle_start(m)
        if topic != T_WS_EVENTS:
            return
        st = self.stations.get(m.get("ws_id"))
        if st is None:
            return
        if kind == "HELLO":
            self.sync_engine(st)
        elif kind == "STARTED":
            self.on_started(st, m)
        elif kind == "REJECTED":
            self.on_rejected(st, m)
        elif kind == "TELEMETRY":
            self.on_telemetry(st, m)
        elif kind == "FINISHED":
            self.finish_session(st, m.get("reason", "STOPPED"), m.get("volume"), m.get("duration"))

    # ------------------------------------------------------------------ socket server (Monitors)
    def socket_server(self):
        while not self.stop.is_set():
            try:
                conn, addr = self.srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self.handle_link, args=(conn, addr), daemon=True).start()
        self.srv.close()

    def handle_link(self, conn: socket.socket, addr):
        """One Monitor connection: REG once, then STATUS every second."""
        link = object()
        ws_id = None
        ch = None
        try:
            ch = Channel.accept(conn, timeout=LINK_TIMEOUT)
            while not self.stop.is_set():
                msg = ch.recv()
                if msg is None:
                    break
                parts = msg.split("#")
                if parts[0] == "REG":
                    new_id_, answer = self.register(parts, link)
                    if new_id_ is not None:
                        ws_id = new_id_
                    ch.send(answer)
                    if ws_id is None:
                        break
                elif parts[0] == "STATUS" and ws_id is not None and len(parts) >= 3 and parts[2] in ("OK", "KO", "WAIT"):
                    self.set_health(self.stations[ws_id], parts[2])
                    ch.send("OK")
                else:
                    ch.send("KO#mensaje no válido")
        except OSError:
            pass
        finally:
            if ws_id is not None:
                self.link_lost(ws_id, link)
            if ch:
                ch.close()
            else:
                conn.close()

    def register(self, parts: list, link) -> tuple:
        # REG#id#name#x#y#token
        try:
            ws_id, name, x, y = int(parts[1]), parts[2], float(parts[3]), float(parts[4])
        except (IndexError, ValueError):
            return None, "KO#registro mal formado"
        token = parts[5] if len(parts) > 5 else ""
        expected = os.environ.get("WM_TOKEN")
        if expected and token != expected:
            self.log(f"{ws_name(ws_id)}: registro RECHAZADO (autenticación fallida)")
            return None, "KO#autenticación fallida"
        with self.lock:
            st = self.stations.get(ws_id)
            if st and st["connected"]:
                return None, "KO#estación ya conectada"
            if st is None:
                self.sql("INSERT INTO ws(id, x, y, name, blocked) VALUES (?, ?, ?, ?, 0)", (ws_id, x, y, name))
                st = self.stations[ws_id] = self.new_station(ws_id, name, x, y, 0)
                self.log(f"{ws_name(ws_id)}: ALTA de nueva estación en '{name}'")
            else:
                self.sql("UPDATE ws SET x = ?, y = ?, name = ? WHERE id = ?", (x, y, name, ws_id))
                st.update(name=name, x=x, y=y)
            st.update(connected=True, link=link, health="WAIT")
        self.log(f"{ws_name(ws_id)}: monitor autenticado en CENTRAL")
        self.publish_state(st)
        return ws_id, "OK"

    def link_lost(self, ws_id: int, link):
        with self.lock:
            st = self.stations.get(ws_id)
            if not st or st["link"] is not link:
                return
            pending = st["pending"]
            st["pending"] = None
        self.finish_session(st, "DISCONNECTED")
        with self.lock:
            st.update(connected=False, link=None, health="WAIT")
        if pending:
            self.tell(pending["req"], "DENIED", f"Riego DENEGADO: {ws_name(ws_id)} se ha desconectado")
        self.log(f"{ws_name(ws_id)}: DESCONECTADA")
        self.publish_state(st)

    # ------------------------------------------------------------------ CENTRAL commands (panel)
    def command(self, cmd: str, target) -> int:
        with self.lock:
            ids = sorted(self.stations) if str(target) == "all" else [int(target)]
        done = 0
        for ws_id in ids:
            st = self.stations.get(ws_id)
            if st is None:
                continue
            done += 1
            if cmd == "start":
                self.handle_start({"fo_id": "CENTRAL", "ws_id": ws_id, "req_id": new_id(), "duration": DEFAULT_DURATION})
            elif cmd in ("block", "activate"):
                blocked = cmd == "block"
                with self.lock:
                    st["blocked"] = blocked
                self.sql("UPDATE ws SET blocked = ? WHERE id = ?", (int(blocked), ws_id))
                self.bus.publish(T_WS_CMD, {"type": "BLOCK" if blocked else "ACTIVATE", "ws_id": ws_id}, key=ws_id)
                self.log(f"{ws_name(ws_id)}: " + ("puesta FUERA DE SERVICIO por CENTRAL" if blocked else "ACTIVADA por CENTRAL"))
                self.publish_state(st)
        return done

    # ------------------------------------------------------------------ housekeeping
    def watchdog(self):
        last_snapshot = 0.0
        while not self.stop.wait(1.0):
            now = time.time()
            expired = []
            with self.lock:
                for st in self.stations.values():
                    pending = st["pending"]
                    if pending and now > pending["deadline"]:
                        st["pending"] = None
                        expired.append(pending["req"])
            for req in expired:
                self.tell(req, "DENIED", f"Riego DENEGADO: {ws_name(req['ws_id'])} no respondió a tiempo")
            if now - last_snapshot >= SNAPSHOT_EVERY:
                self.publish_all()
                last_snapshot = now

    # ------------------------------------------------------------------ web panel
    def http_server(self, port: int):
        central = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, code: int, body, ctype: str = "application/json"):
                data = body if isinstance(body, bytes) else body.encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", f"{ctype}; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                if self.path in ("/", "/index.html"):
                    self._send(200, PANEL_HTML, "text/html")
                elif self.path == "/api/state":
                    self._send(200, json.dumps(central.panel_state()))
                elif self.path == "/health":
                    self._send(200, "ok", "text/plain")
                else:
                    self._send(404, "not found", "text/plain")

            def do_POST(self):
                if self.path != "/api/cmd":
                    return self._send(404, "not found", "text/plain")
                token = os.environ.get("WM_ADMIN_TOKEN")
                if token and self.headers.get("X-Token") != token:
                    return self._send(401, '{"error":"unauthorized"}')
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length))
                    cmd, target = body["cmd"], body["target"]
                    if cmd not in ("start", "block", "activate"):
                        raise ValueError(cmd)
                    count = central.command(cmd, target)
                except (ValueError, KeyError, TypeError):
                    return self._send(400, '{"error":"bad request"}')
                self._send(200, json.dumps({"ok": True, "stations": count}))

        httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        httpd.daemon_threads = True
        threading.Thread(target=httpd.serve_forever, daemon=True, name="http").start()
        self.log(f"CENTRAL: panel web en el puerto {port}")

    def run(self):
        self.db_init()
        self.load_stations()
        self.bus.ensure_topics()
        ready = self.bus.listen([T_REQUESTS, T_WS_EVENTS], self.on_kafka, self.stop, "central")
        wait_ready(ready)

        self.srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("0.0.0.0", self.port))
        self.srv.listen(32)
        self.srv.settimeout(1.0)
        threading.Thread(target=self.socket_server, daemon=True, name="sockets").start()
        threading.Thread(target=self.watchdog, daemon=True, name="watchdog").start()
        self.http_server(int(os.environ.get("WM_HTTP_PORT") or os.environ.get("PORT") or 8080))
        self.publish_all()
        self.log(f"CENTRAL en marcha: sockets :{self.port}, kafka {self.bootstrap}")

        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())
        while not self.stop.wait(1.0):
            pass
        self.log("CENTRAL: apagando...")
        self.bus.flush()


def main(*args: str):
    port = args[0] if len(args) > 0 else os.environ.get("WM_PORT", "")
    kafka = args[1] if len(args) > 1 else os.environ.get("WM_KAFKA", "")
    if not port.isdigit() or not kafka:
        print(USAGE_MSG)
        return 1
    host, kport = parse_hostport(kafka)
    Central(int(port), f"{host}:{kport}").run()


PANEL_HTML = '''<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>WM_Central - Panel de riego</title>
<style>
 body{font:14px system-ui,sans-serif;margin:0;background:#eef0f3;color:#111}
 header{background:#1f4e8c;color:#fff;padding:10px 16px;display:flex;justify-content:space-between;align-items:center}
 main{display:flex;gap:14px;padding:14px;flex-wrap:wrap}
 .col{flex:1 1 640px}.side{flex:0 0 260px}
 table{border-collapse:collapse;width:100%;background:#fff}
 th,td{border:1px solid #cfd3d9;padding:6px 8px;text-align:left}
 th{background:#dde2ea}
 .AVAILABLE{background:#9be59b}.LEAK{background:#f05a5a;color:#fff}
 .OUT_OF_SERVICE{background:#f7a843}.DISCONNECTED{background:#bdbdbd}
 .WATERING{background:#9be59b;animation:blink 1s steps(2,start) infinite}
 @keyframes blink{50%{background:#e6f9e6}}
 button{padding:4px 9px;margin:1px;border:1px solid #888;border-radius:4px;background:#fff;cursor:pointer}
 button:hover{background:#e8eefc}
 .box{background:#fff;border:1px solid #cfd3d9;margin-bottom:12px}
 .box h3{margin:0;padding:6px 10px;background:#dde2ea;font-size:13px}
 .box div{padding:6px 10px}
 .row{display:flex;justify-content:space-between;padding:2px 0}
 pre{margin:0;background:#101418;color:#c9f5c9;padding:10px;height:230px;overflow:auto;font-size:12px}
 .bar{margin-bottom:8px}
</style></head><body>
<header><b>WM_Central &mdash; Panel de monitorizaci&oacute;n</b><span id="info"></span></header>
<main>
 <div class="col">
  <div class="bar">
   <button onclick="cmd('start','all')">Regar todas</button>
   <button onclick="cmd('block','all')">Bloquear todas</button>
   <button onclick="cmd('activate','all')">Activar todas</button>
  </div>
  <table><thead><tr><th>ID</th><th>Ubicaci&oacute;n</th><th>Estado</th><th>Caudal (L/min)</th>
   <th>Vol. acumulado (L)</th><th>Operario</th><th>Acciones</th></tr></thead><tbody id="rows"></tbody></table>
  <div class="box" style="margin-top:12px"><h3>MENSAJES DE LA APLICACI&Oacute;N</h3><pre id="log"></pre></div>
 </div>
 <div class="side">
  <div class="box"><h3>RESUMEN</h3><div id="sum"></div></div>
  <div class="box"><h3>LEYENDA</h3><div>
   <div class="AVAILABLE">Disponible</div><div class="WATERING">Regando (parpadeo)</div>
   <div class="LEAK">Fuga</div><div class="OUT_OF_SERVICE">Fuera de Servicio</div>
   <div class="DISCONNECTED">Desconectada</div></div></div>
 </div>
</main>
<script>
const ES={AVAILABLE:'Disponible',WATERING:'Regando',LEAK:'Fuga',OUT_OF_SERVICE:'Fuera de Servicio',DISCONNECTED:'Desconectada'};
let token='';
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function cmd(c,t){
  const r=await fetch('/api/cmd',{method:'POST',headers:{'Content-Type':'application/json','X-Token':token},
    body:JSON.stringify({cmd:c,target:String(t)})});
  if(r.status===401){token=prompt('Token de administración:')||'';}
  refresh();
}
async function refresh(){
  let d; try{d=await (await fetch('/api/state')).json();}catch(e){return;}
  document.getElementById('rows').innerHTML=d.stations.map(s=>{
    const w=s.status==='WATERING';
    const id='WS-'+String(s.ws_id).padStart(2,'0');
    return `<tr class="${s.status}"><td><b>${id}</b></td><td>${esc(s.name)}</td><td><b>${ES[s.status]}</b></td>
     <td>${w?s.flow.toFixed(1):'-'}</td><td>${w?s.volume.toFixed(1):'-'}</td><td>${w?esc(s.fo_id):'-'}</td>
     <td><button onclick="cmd('start',${s.ws_id})">Regar</button><button onclick="cmd('block',${s.ws_id})">Bloquear</button>
     <button onclick="cmd('activate',${s.ws_id})">Activar</button></td></tr>`;}).join('');
  document.getElementById('sum').innerHTML=Object.keys(ES).map(k=>`<div class="row"><span>${ES[k]}</span><b>${d.counts[k]}</b></div>`).join('');
  const log=document.getElementById('log'), bottom=log.scrollTop+log.clientHeight>=log.scrollHeight-8;
  log.textContent=d.messages.slice().reverse().join('\\n'); if(bottom) log.scrollTop=0;
  document.getElementById('info').textContent=`Kafka ${d.kafka} | sockets :${d.port} | activo ${d.uptime}s | ${d.stations.length} estaciones`;
}
refresh(); setInterval(refresh,1000);
</script></body></html>
'''
