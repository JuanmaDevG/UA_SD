"""Field Operator

   (Later a proper description)
"""

import os
from queue import Queue
from confluent_kafka import Consumer, KafkaException, KafkaError, Producer


import sys
import threading
import time
from types import SimpleNamespace

from math import integer

EXIT = 0x00
STX = 0x01
ETX = 0x02
NACK = 0x03
ACK = 0x04

FO_WATER = 0x08 # bytes[0x08, id_ws(1 byte)]

#Producer queue
producer_queue = Queue()


USAGE_MSG = """
Usage:
    WM_FO <kafka_host>:<kafka_port>
    WM_FO here

Arguments:
    <kafka_host>    - Kafka server host name
    <kafka_port>    - Kafka server port
    here            - $ WM_FieldOperator localhost:8000
"""

def get_netparams(args: tuple[str, ...]) -> str:
    if len(args) == 0:
        print(USAGE_MSG)
        sys.exit(1)
    kafka: str
    if len(args) == 1 and args[0] == "here":
        kafka = "localhost:8000"
    elif (
        len(args) == 1
        and (l := args[0].find(":")) not in (-1, 0, len(args[0]) - 1)
        and args[0][l + 1 :].isnumeric()
    ):
        kafka = args[0]
    else:
        print(USAGE_MSG)
        sys.exit(1)

    return kafka

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def kafka_producer(producer):

    while True:
        item = producer_queue.get()

        if item is None:
            break

        message = item
        try:
            producer.produce(topic = 'Start_wat', value= message)
        except Exception as error:
            print(f"Error al enviar mensaje: {error}")

        producer_queue.task_done()


def send_instruction(id_estacion):
    message = bytes([0x08, id_estacion.to_bytes()])
    producer_queue.put(message)
    

def main(*args):
    kafka = get_netparams(args)

    producer = Producer({'Puerto Kafka': kafka})


    threading.Thread(
        target=kafka_producer,
        name='ws-watering',
        args=(producer,),
        daemon=True
    ).start()

    
if __name__ == "__main__":
    main(sys.argv[1:])