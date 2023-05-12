
import json
import os
import socket
import logging
from time import sleep
from common.Middleware import Middleware

EOFTRIPSLISTENER = "eoftripslistener"
TRIPS = "trips"
TRIPSEJ2 = "tripsej2"
TRIPSEJ3 = "tripsej3"
EJ1TRIPSSOLVER = "ej1tripssolver"
EJ2TRIPSSOLVER = "ej2tripssolver"
EJ3TRIPSSOLVER = "ej3tripssolver"

class EofTripsListener:
    def __init__(self):
        self._sigterm = False

        self._remaining_eof = {
            TRIPSEJ2: int(os.getenv('TE2FCANT', "")),
            TRIPSEJ3: int(os.getenv('TE3FCANT', "")),
            TRIPS: int(os.getenv('TBRKCANT', ""))
        }

        self._cant_solvers = {
            EJ1TRIPSSOLVER: int(os.getenv('EJ1TRIPSCANT', "")),
            EJ2TRIPSSOLVER: int(os.getenv('EJ2TRIPSCANT', "")),
            EJ3TRIPSSOLVER: int(os.getenv('EJ3TRIPSCANT', "")),
        }

        self._middleware: Middleware = None

    def _create_RabbitMQ_Connection(self):
        logging.info(f'action: create rabbitmq connections | result: in_progress')
        self._middleware = Middleware()

        self._middleware.queue_declare(queue=EOFTRIPSLISTENER, durable=True)
        self._middleware.queue_declare(queue=EJ1TRIPSSOLVER, durable=True)
        self._middleware.queue_declare(queue=EJ2TRIPSSOLVER, durable=True)
        self._middleware.queue_declare(queue=EJ3TRIPSSOLVER, durable=True)
        logging.info(f'action: create rabbitmq connections | result: success')

    def _sigterm_handler(self, _signo, _stack_frame):
        self._sigterm = True
        if self._middleware is not None:
            self._middleware.close()
        exit(0)

    def run(self):
        try:
            self._create_RabbitMQ_Connection()
            logging.info(f'action: run | result: in_progress')
            self._middleware.basic_qos(prefetch_count=1)
            self._middleware.recv_message(queue=EOFTRIPSLISTENER, callback=self._callback)
            self._middleware.start_consuming()
        except Exception as e:
            logging.error(f'action: run | result: error | error: {e}')
            if self._middleware is not None:
                self._middleware.close()
            exit(0)

    def _callback(self, ch, method, properties, body):
        self._proccess_eof(body.decode("utf-8"))
        self._middleware.send_ack(method.delivery_tag)
        if self._finished(): self._exit()

    def _proccess_eof(self, body):
        finished = False
        self._remaining_eof[body] -= 1
        logging.info(f'action: proccess eof | result: in_progress | body: {body} | remaining: {self._remaining_eof[body]}')
        if self._remaining_eof[body] == 0:
            finished = self._send_eofs(body)

    def _send_eofs(self, body):
        if body == TRIPSEJ2:
            for _ in range(self._cant_solvers[EJ2TRIPSSOLVER]):
                self._send(EJ2TRIPSSOLVER, self._get_eof())
        elif body == TRIPSEJ3:
            for _ in range(self._cant_solvers[EJ3TRIPSSOLVER]):
                self._send(EJ3TRIPSSOLVER, self._get_eof())
        elif body == TRIPS:
            for _ in range(self._cant_solvers[EJ1TRIPSSOLVER]):
                self._send(EJ1TRIPSSOLVER, self._get_eof())
        else:
            logging.error(f'action: send eof | result: error | error: invalid body')
            return
        logging.info(f'action: send eof | result: success | body: {body}')

    def _finished(self):
        return self._remaining_eof[TRIPSEJ2] == 0 and self._remaining_eof[TRIPSEJ3] == 0 and self._remaining_eof[TRIPS] == 0

    def _send(self, queue, data):
        self._middleware.send_message(queue=queue, data=data)

    def _get_eof(self):
        return json.dumps({
            "type": "eof",
        })

    def _exit(self):
        self._middleware.stop_consuming()
        self._middleware.close()
