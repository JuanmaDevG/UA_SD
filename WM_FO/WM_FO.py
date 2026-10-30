"""Field Operator

   (Later a proper description)
"""

import os
from pathlib import Path
from queue import Queue
from confluent_kafka import Consumer, KafkaException, KafkaError, Producer


import sys
import threading
import time
from types import SimpleNamespace

EXIT = 0x00
STX = 0x01
ETX = 0x02
NACK = 0x03
ACK = 0x04

FO_WATER = 0x08 # bytes[0x08, id_ws(1 byte)]


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

def run_kafka(kafka):
    producer = Producer({'Puerto Kafka': kafka})

    #Cambiar el while True por la comprobación de que se haya pulsado el botón de comenzar riego
    while True:
        #Cambiar el value por los bytes de arriba
        producer.produce('Watering', value = 'Hola', callback=delivery_report)
        producer.poll(0)

def main(*args):
    kafka = get_netparams(args)

    run_kafka(kafka)


if __name__ == "__main__":
    main(sys.argv[1:])