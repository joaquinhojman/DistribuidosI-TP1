import json
import logging
import os
from time import sleep
import pika

class Ej2Solver:
    def __init__(self, EjSolver, channel):
        self._EjSolver = EjSolver
        self._channel = channel

    def run(self):
        logging.info(f'action: run_Ej2Solver | result: in_progress')
        self._channel.basic_consume(queue=self._EjSolver, on_message_callback=self._callback)

    def _callback(self, ch, method, properties, body):
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == "station":
            pass
        elif data["type"] == "trip":
            pass
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')

