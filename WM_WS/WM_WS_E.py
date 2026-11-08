"""Water Management Watering Station - Engine

The engine simulates the physical station. It only talks to its monitor (TCP,
length-prefixed frames), and the monitor relays everything to CENTRAL via Kafka.
    - Accepts or rejects watering requests (operators and CENTRAL)
    - While watering, reports WS_WATERINFO every second
    - Throws random problems (LEAK, OUT_OF_SERVICE) that get fixed after a while
    - Menu to force problems by hand

Frames between monitor and engine: bytes[length, instruction (length bytes)]
"""

from random import randint, random
import socket
import sys
import threading
import time
from types import SimpleNamespace


USAGE_MSG = """
Usage:
    WM_WS_E <monitor_host>:<monitor_port>
Arguments:
    <monitor_host>  - Monitor host name
    <monitor_port>  - Monitor port
Example:
    $ WM_WS_E localhost:6001
"""


# System message coloring
OK = "\033[1;32m"
WARNING = "\033[1;33m"
ERROR = "\033[1;31m"


# Water station states
DISCONNECTED = 0
AVAILABLE = 1
WATERING = 2
LEAK = 3
OUT_OF_SERVICE = 4
WS_STATES = (DISCONNECTED, AVAILABLE, WATERING, LEAK, OUT_OF_SERVICE)
WS_STATE_NAMES = ["DISCONNECTED", "AVAILABLE", "WATERING", "LEAK", "OUT_OF_SERVICE"]


# General instructions
STX = 0x01
ETX = 0x02
NACK = 0x03
ACK = 0x04


# Instructions used by the engine
OP_REQ_WATERING = 0x09  # bytes[0x09, op_id, ws_id]
WS_ANSWER_WATERING = 0x0A  # bytes[0x0A, ACK/NACK, op_id, ws_id]
C_REQ_WATERING = 0x0B  # bytes[0x0B, ws_id]
WS_C_ANSWER_WATERING = 0x0C  # bytes[0x0C, ACK/NACK]
C_WS_SHUTDOWN = 0x0D  # bytes[0x0D, ws_id]
WS_GOODBYE = 0x0F  # bytes[0x0F, ws_id]
C_PING_WS = 0x11  # bytes[0x11, ws_id]
WS_PUBLISH_STATE = 0x12  # bytes[0x12, ws_id, state]
WS_I_AM = 0x19  # bytes[0x19, ws_id, x, y, STX, name (n bytes), ETX, state]
WS_WATERINFO = 0x1B  # bytes[0x1B, ws_id, liters_per_minute, accum]


WATERING_TIME = 20  # seconds a watering lasts
PROBLEM_CHANCE = 0.03  # chance per second of a random problem while AVAILABLE
FIX_CHANCE = 0.1  # chance per second of a problem getting fixed


def stamp() -> str:
    return time.strftime("%H:%M:%S")


def tell(mtype: str, msg: str):
    print(
        f"[{stamp()}] WS_E: {mtype}{msg}",
        end="\033[1;0m\n",
        file=sys.stdout if mtype != ERROR else sys.stderr,
    )


def get_params(args: tuple[str, ...]) -> tuple[str, int]:
    if len(args) != 1 or (l := args[0].find(":")) in (-1, 0, len(args[0]) - 1) or not args[
        0
    ][l + 1 :].isnumeric():
        print(USAGE_MSG)
        sys.exit(1)
    return args[0][:l], int(args[0][l + 1 :])


def send_frame(conn: socket.socket, ins: bytes) -> None:
    conn.sendall(bytes([len(ins)]) + ins)


def recv_exactly(conn: socket.socket, n: int) -> bytes | None:
    data = b""
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def recv_frame(conn: socket.socket) -> bytes | None:
    """Returns None when the other side closes the connection"""
    header = recv_exactly(conn, 1)
    if header is None:
        return None
    return recv_exactly(conn, header[0])


def main(*args):
    monitor = get_params(args)
    try:
        conn = socket.create_connection(monitor, timeout=5)
    except OSError as err:
        tell(ERROR, f"no se puede conectar con el monitor {monitor}: {err}")
        sys.exit(1)
    conn.settimeout(None)

    # The monitor introduces the station first
    i_am = recv_frame(conn)
    if not i_am or i_am[0] != WS_I_AM or len(i_am) < 7 or (name_end := i_am.find(ETX, 5)) == -1:
        tell(ERROR, f"el monitor no ha enviado WS_I_AM: {i_am!r}")
        sys.exit(1)
    ws_id = i_am[1]
    tell(OK, f"estación {ws_id} ({i_am[5:name_end].decode(errors='replace')}) lista")

    station = SimpleNamespace(state=AVAILABLE, watering_left=0, liters_per_minute=0, accum=0)
    lock = threading.Lock()  # protects 'station' and writes on 'conn'
    shutdown_event = threading.Event()

    def set_state(state: int) -> None:
        """Call with 'lock' held"""
        station.state = state
        tell(WARNING if state in (LEAK, OUT_OF_SERVICE) else OK, f"estado -> {WS_STATE_NAMES[state]}")
        send_frame(conn, bytes([WS_PUBLISH_STATE, ws_id, state]))

    def start_watering() -> bool:
        """Call with 'lock' held"""
        if station.state != AVAILABLE:
            return False
        station.watering_left = WATERING_TIME
        station.liters_per_minute = randint(5, 15)
        station.accum = 0
        set_state(WATERING)
        return True

    # Commands from the monitor
    def listen() -> None:
        while not shutdown_event.is_set():
            try:
                ins = recv_frame(conn)
            except OSError:
                ins = None
            if ins is None:
                tell(ERROR, "el monitor se ha desconectado")
                shutdown_event.set()
                return
            if not ins:
                continue
            opcode = ins[0]

            with lock:
                if opcode == OP_REQ_WATERING and len(ins) == 3:
                    # [OP_REQ_WATERING, op_id, ws_id]
                    answer = ACK if start_watering() else NACK
                    tell(OK, f"operador {ins[1]} pide regar: {'ACK' if answer == ACK else 'NACK'}")
                    send_frame(conn, bytes([WS_ANSWER_WATERING, answer, ins[1], ws_id]))
                elif opcode == C_REQ_WATERING and len(ins) == 2:
                    # [C_REQ_WATERING, ws_id]
                    answer = ACK if start_watering() else NACK
                    tell(OK, f"CENTRAL pide regar: {'ACK' if answer == ACK else 'NACK'}")
                    send_frame(conn, bytes([WS_C_ANSWER_WATERING, answer]))
                elif opcode == C_PING_WS and len(ins) == 2:
                    # [C_PING_WS, ws_id]: answer with the current state
                    send_frame(conn, bytes([WS_PUBLISH_STATE, ws_id, station.state]))
                elif opcode == C_WS_SHUTDOWN and len(ins) == 2:
                    # [C_WS_SHUTDOWN, ws_id]
                    tell(WARNING, "CENTRAL ha ordenado apagar la estación")
                    shutdown_event.set()
                    return
                else:
                    tell(ERROR, f"instrucción desconocida: {ins!r}")

    # One tick per second: watering progress and random problems
    def simulate() -> None:
        while not shutdown_event.wait(1):
            with lock:
                try:
                    if station.state == WATERING:
                        station.accum = min(
                            255, station.accum + max(1, station.liters_per_minute // 60)
                        )
                        send_frame(
                            conn,
                            bytes(
                                [WS_WATERINFO, ws_id, station.liters_per_minute, station.accum]
                            ),
                        )
                        station.watering_left -= 1
                        if station.watering_left <= 0:
                            set_state(AVAILABLE)
                        elif random() < PROBLEM_CHANCE:
                            set_state(LEAK)  # a leak stops the watering
                    elif station.state == AVAILABLE and random() < PROBLEM_CHANCE:
                        set_state(LEAK if random() < 0.5 else OUT_OF_SERVICE)
                    elif station.state in (LEAK, OUT_OF_SERVICE) and random() < FIX_CHANCE:
                        set_state(AVAILABLE)
                except OSError:
                    shutdown_event.set()

    threading.Thread(target=listen, name="ws-listener", daemon=True).start()
    threading.Thread(target=simulate, name="ws-simulator", daemon=True).start()

    print("Comandos: l (fuga) | o (fuera de servicio) | f (reparar) | s (estado) | q (salir)")
    try:
        while not shutdown_event.is_set():
            cmd = input("> ").strip()
            with lock:
                if cmd == "l":
                    set_state(LEAK)
                elif cmd == "o":
                    set_state(OUT_OF_SERVICE)
                elif cmd == "f":
                    set_state(AVAILABLE)
                elif cmd == "s":
                    print(f"  {WS_STATE_NAMES[station.state]}")
                elif cmd == "q":
                    break
                elif cmd:
                    print("Comandos: l | o | f | s | q")
    except (KeyboardInterrupt, EOFError, OSError):
        pass
    finally:
        tell(OK, "Saliendo...")
        shutdown_event.set()
        try:
            with lock:
                send_frame(conn, bytes([WS_GOODBYE, ws_id]))
        except OSError:
            pass
        conn.close()


if __name__ == "__main__":
    main(*sys.argv[1:])
