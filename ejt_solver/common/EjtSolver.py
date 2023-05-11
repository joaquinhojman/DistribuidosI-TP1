import logging
import os
from time import sleep
from common.Middleware import Middleware

from common.Ej1tSolver import Ej1tSolver
from common.Ej2tSolver import Ej2tSolver
from common.Ej3tSolver import Ej3tSolver

class EjtSolver:
    def __init__(self, ejtsolver, ej1tsolver, ej2tsolver, ej3tsolver):
        self._sigterm = False
        self._EjtSolver = ejtsolver
        self._ej1tsolver = ej1tsolver
        self._ej2tsolver = ej2tsolver
        self._ej3tsolver = ej3tsolver

        self._middleware: Middleware = None

    def _initialize_rabbitmq(self):
        logging.info(f'action: initialize_rabbitmq | result: in_progress | EjtSolver: {self._EjtSolver}')
        self._middleware = Middleware()
        logging.info(f'action: initialize_rabbitmq | result: success | EjtSolver: {self._EjtSolver}')

    def _sigterm_handler(self, _signo, _stack_frame):
        self._sigterm = True
        if self._middleware is not None:
            self._middleware.close()
        exit(0)

    def run(self):
        try:
            self._initialize_rabbitmq()
            logging.info(f'action: run | result: in_progress | EjSolver: {self._EjtSolver}')
            if self._EjtSolver == self._ej1tsolver:
                ej1tSolver = Ej1tSolver(self._EjtSolver, self._middleware)
                ej1tSolver.run()
            elif self._EjtSolver == self._ej2tsolver:
                ej2tSolver = Ej2tSolver(self._EjtSolver, self._middleware)
                ej2tSolver.run()
            elif self._EjtSolver == self._ej3tsolver:
                ej3tSolver = Ej3tSolver(self._EjtSolver, self._middleware)
                ej3tSolver.run()
            else:
                logging.error(f'action: run | result: error | EjtSolver: {self._EjtSolver} | error: Invalid filter type')
                raise Exception("Invalid filter type")
        except Exception as e:
            logging.error(f'action: run | result: error | EjtSolver: {self._EjtSolver} | error: {e}')
            if self._middleware is not None:
                self._middleware.close()
            exit(0)
