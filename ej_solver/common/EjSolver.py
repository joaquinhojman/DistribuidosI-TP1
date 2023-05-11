import logging
import os
from time import sleep
from common.Middleware import Middleware
from common.Ej1Solver import Ej1Solver
from common.Ej2Solver import Ej2Solver
from common.Ej3Solver import Ej3Solver

RESULTS = "results"

class EjSolver:
    def __init__(self, EjSolver, ej1solver, ej2solver, ej3solver):
        self._sigterm = False
        self._EjSolver = EjSolver
        self._ej1solver = ej1solver
        self._ej2solver = ej2solver
        self._ej3solver = ej3solver

        self._middleware: Middleware = None

    def _initialize_rabbitmq(self):
        logging.info(f'action: initialize_rabbitmq | result: in_progress | EjSolver: {self._EjSolver}')
        self._middleware = Middleware()

        self._middleware.queue_declare(queue=self._EjSolver, durable=True)
        self._middleware.queue_declare(queue=RESULTS, durable=True)
        logging.info(f'action: initialize_rabbitmq | result: success | EjSolver: {self._EjSolver}')

    def _sigterm_handler(self, _signo, _stack_frame):
        self._sigterm = True
        if self._middleware is not None:
            self._middleware.close()
        exit(0)

    def run(self):
        try:
            self._initialize_rabbitmq()
            logging.info(f'action: run | result: in_progress | EjSolver: {self._EjSolver}')
            if self._EjSolver == self._ej1solver:
                ej1Solver = Ej1Solver(self._EjSolver, self._middleware)
                ej1Solver.run()
            elif self._EjSolver == self._ej2solver:
                ej2Solver = Ej2Solver(self._EjSolver, self._middleware)
                ej2Solver.run()
            elif self._EjSolver == self._ej3solver:
                ej3Solver = Ej3Solver(self._EjSolver, self._middleware)
                ej3Solver.run()
            else:
                logging.error(f'action: run | result: error | EjSolver: {self._EjSolver} | error: Invalid filter type')
                raise Exception("Invalid filter type")
        except Exception as e:
            logging.error(f'action: run | result: error | EjSolver: {self._EjSolver} | error: {e}')
            if self._middleware is not None:
                self._middleware.close()
            exit(0)
