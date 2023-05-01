import logging
import os
from time import sleep
import pika
from common.Ej1Solver import Ej1Solver
from common.Ej2Solver import Ej2Solver
from common.Ej3Solver import Ej3Solver

RESULTS = "results"

class EjSolver:
    def __init__(self, EjSolver, ej1solver, ej2solver, ej3solver):
        self._EjSolver = EjSolver
        self._ej1solver = ej1solver
        self._ej2solver = ej2solver
        self._ej3solver = ej3solver

        self._channel = None
        self._initialize_rabbitmq()

    def _initialize_rabbitmq(self):
        logging.info(f'action: initialize_rabbitmq | result: in_progress | EjSolver: {self._EjSolver}')
        while self._channel is None:
            sleep(15)
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq'))
                channel = connection.channel()

                channel.queue_declare(queue=self._EjSolver, durable=True)
                channel.queue_declare(queue=RESULTS, durable=True)
                self._channel = channel
            except Exception as e:
                pass
        logging.info(f'action: initialize_rabbitmq | result: success | EjSolver: {self._EjSolver}')

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress | EjSolver: {self._EjSolver}')
        if self._channel is not None:
            self._channel.close()
        logging.info(f'action: Handle SIGTERM | result: success | EjSolver: {self._EjSolver}')

    def run(self):
        try:
            logging.info(f'action: run | result: in_progress | EjSolver: {self._EjSolver}')
            if self._EjSolver == self._ej1solver:
                ej1Solver = Ej1Solver(self._EjSolver, self._channel)
                ej1Solver.run()
            elif self._EjSolver == self._ej2solver:
                ej2Solver = Ej2Solver(self._EjSolver, self._channel)
                ej2Solver.run()
            elif self._EjSolver == self._ej3solver:
                ej3Solver = Ej3Solver(self._EjSolver, self._channel)
                ej3Solver.run()
            else:
                logging.error(f'action: run | result: error | EjSolver: {self._EjSolver} | error: Invalid filter type')
                raise Exception("Invalid filter type")
        except Exception as e:
            logging.error(f'action: run | result: error | EjSolver: {self._EjSolver} | error: {e}')
            if self._channel is not None:
                self._channel.close()
