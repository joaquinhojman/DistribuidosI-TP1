import logging
import os
from time import sleep
import pika

from common.Ej1tSolver import Ej1tSolver
from common.Ej2tSolver import Ej2tSolver
from common.Ej3tSolver import Ej3tSolver

class EjtSolver:
    def __init__(self, ejtsolver, ej1tsolver, ej2tsolver, ej3tsolver):
        self._EjtSolver = ejtsolver
        self._ej1tsolver = ej1tsolver
        self._ej2tsolver = ej2tsolver
        self._ej3tsolver = ej3tsolver

        self._channel = None
        self._initialize_rabbitmq()

    def _initialize_rabbitmq(self):
        logging.info(f'action: initialize_rabbitmq | result: in_progress | EjtSolver: {self._EjtSolver}')
        while self._channel is None:
            sleep(15)
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq'))
                channel = connection.channel()

                channel.queue_declare(queue=self._EjtSolver, durable=True)
                self._channel = channel
            except Exception as e:
                pass
        logging.info(f'action: initialize_rabbitmq | result: success | EjtSolver: {self._EjtSolver}')

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress | EjtSolver: {self._EjtSolver}')
        if self._channel is not None:
            self._channel.close()
        logging.info(f'action: Handle SIGTERM | result: success | EjtSolver: {self._EjtSolver}')

    def run(self):
        try:
            logging.info(f'action: run | result: in_progress | EjSolver: {self._EjtSolver}')
            if self._EjtSolver == self._ej1tsolver:
                ej1tSolver = Ej1tSolver(self._EjtSolver, self._channel, )
                ej1tSolver.run()
            elif self._EjtSolver == self._ej2tsolver:
                ej2tSolver = Ej2tSolver(self._EjtSolver, self._channel, )
                ej2tSolver.run()
            elif self._EjtSolver == self._ej3tsolver:
                ej3tSolver = Ej3tSolver(self._EjtSolver, self._channel, )
                ej3tSolver.run()
            else:
                logging.error(f'action: run | result: error | EjtSolver: {self._EjtSolver} | error: Invalid filter type')
                raise Exception("Invalid filter type")
        except Exception as e:
            logging.error(f'action: run | result: error | EjtSolver: {self._EjtSolver} | error: {e}')
            if self._channel is not None:
                self._channel.close()
