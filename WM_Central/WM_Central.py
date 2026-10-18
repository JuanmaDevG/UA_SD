import sys
import socket
from confluent_kafka import Consumer, Producer
from threading import Thread

USAGE_MSG = '''
Usage:
    WM_Central <login_host>:<login_port> <qm_host>:<qm_port>
'''

DB_SCHEMA = '''
CREATE TABLE IF NOT EXISTS ws (
    id INTEGER PRIMARY KEY,
    x REAL, y REAL,
    name TEXT
    )
'''

running = True

p_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'WM'
}

c_config = {
    'bootstrap.servers': 'localhost:9092',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'group.id': 'WM'
}

def wsauth_server(host: str, port: int):
    global running
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))

    while(running):
        client, client_addr = server.accept()
        # TODO: access db
        client.close()

    server.close()

def run(*args: tuple[object, ...]):
    global running
    if len(args) != 3:
        print(USAGE_MSG)
        return

    Thread(None, wsauth_server).start()

    config = {
        'bootstrap.servers': args[2],
        'group.id': 'WM'
    }
    prod = Producer(config)


if __name__ == '__main__':
    pass
