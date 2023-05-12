
import os
import socket
import logging
from time import sleep
from common.Middleware import Middleware

EOFLISTENER = "eoflistener"
WEATHER = "weather"
STATIONS = "stations"
TRIPS = "trips"
WEATHEREJ1FILTER = "weatherej1"
STATIONSEJ2FILTER = "stationsej2"
TRIPSEJ2FILTER = "tripsej2"
STATIONSEJ3FILTER = "stationsej3"
TRIPSEJ3FILTER = "tripsej3"
EOF = "eof"

class EofListener:
    def __init__(self):
        self._sigterm = False

        self._remaining_brokers_eof = {
            WEATHER: int(os.getenv('WBRKCANT', "")),
            STATIONS: int(os.getenv('SBRKCANT', "")),
            TRIPS: int(os.getenv('TBRKCANT', ""))
        }

        self._cant_filters = {
            WEATHEREJ1FILTER: int(os.getenv('WE1FCANT', "")),
            STATIONSEJ2FILTER: int(os.getenv('SE2FCANT', "")),
            TRIPSEJ2FILTER: int(os.getenv('TE2FCANT', "")),
            STATIONSEJ3FILTER: int(os.getenv('SE3FCANT', "")),
            TRIPSEJ3FILTER: int(os.getenv('TE3FCANT', ""))
        }

        self._middleware: Middleware = None

    def _create_RabbitMQ_Connection(self):
        logging.info(f'action: create rabbitmq connections | result: in_progress')
        self._middleware = Middleware()

        self._middleware.queue_declare(queue=EOFLISTENER, durable=True)
        self._middleware.queue_declare(queue=WEATHEREJ1FILTER, durable=True)
        self._middleware.queue_declare(queue=STATIONSEJ2FILTER, durable=True)
        self._middleware.queue_declare(queue=TRIPSEJ2FILTER, durable=True)
        self._middleware.queue_declare(queue=STATIONSEJ3FILTER, durable=True)
        self._middleware.queue_declare(queue=TRIPSEJ3FILTER, durable=True)
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
            self._middleware.recv_message(queue=EOFLISTENER, callback=self._callback)
            self._middleware.start_consuming()
        except Exception as e:
            logging.error(f'action: run | result: error | error: {e}')        
            if self._middleware is not None:
                self._middleware.close()
            exit(0)

    def _callback(self, ch, method, properties, body):
        finished = self._proccess_eof(body.decode("utf-8"))
        self._middleware.send_ack(method.delivery_tag)
        if finished: self._exit()

    def _proccess_eof(self, body):
        finished = False
        self._remaining_brokers_eof[body] -= 1
        if self._remaining_brokers_eof[body] == 0:
            finished = self._send_eofs(body)
        return finished

    def _send_eofs(self, body):
        if body == WEATHER:
            for _ in range(self._cant_filters[WEATHEREJ1FILTER]):
                self._send(WEATHEREJ1FILTER, EOF + "," + body)
        elif body == STATIONS:
            for _ in range(self._cant_filters[STATIONSEJ2FILTER]):
                self._send(STATIONSEJ2FILTER, EOF + "," + body)
            for _ in range(self._cant_filters[STATIONSEJ3FILTER]):
                self._send(STATIONSEJ3FILTER, EOF + "," + body)
        elif body == TRIPS:
            for _ in range(self._cant_filters[TRIPSEJ2FILTER]):
                self._send(TRIPSEJ2FILTER, EOF + "," + body)
            for _ in range(self._cant_filters[TRIPSEJ3FILTER]):
                self._send(TRIPSEJ3FILTER, EOF + "," + body)
            return True
        else:
            logging.error(f'action: send eof | result: error | error: invalid body')
            return
        logging.info(f'action: send eof | result: success | body: {body}')
        return False

    def _send(self, queue, data):
        self._middleware.send_message(queue=queue, data=data)

    def _exit(self):
        self._middleware.stop_consuming()
        self._middleware.close()
