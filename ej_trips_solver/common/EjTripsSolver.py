import logging
from common.Middleware import Middleware

from common.Ej1TripsSolver import Ej1TripsSolver
from common.Ej2TripsSolver import Ej2TripsSolver
from common.Ej3TripsSolver import Ej3TripsSolver

class EjTripsSolver:
    def __init__(self, ejtripssolver, ej1tripssolver, ej2tripssolver, ej3tripssolver):
        self._sigterm = False
        self._EjTripsSolver = ejtripssolver
        self._ej1tripssolver = ej1tripssolver
        self._ej2tripssolver = ej2tripssolver
        self._ej3tripssolver = ej3tripssolver

        self._middleware: Middleware = None

    def _initialize_rabbitmq(self):
        logging.info(f'action: initialize_rabbitmq | result: in_progress | _EjTripsSolver: {self._EjTripsSolver}')
        self._middleware = Middleware()
        logging.info(f'action: initialize_rabbitmq | result: success | _EjTripsSolver: {self._EjTripsSolver}')

    def _sigterm_handler(self, _signo, _stack_frame):
        self._sigterm = True
        if self._middleware is not None:
            self._middleware.close()
        exit(0)

    def run(self):
        try:
            self._initialize_rabbitmq()
            logging.info(f'action: run | result: in_progress | EjSolver: {self._EjTripsSolver}')
            if self._EjTripsSolver == self._ej1tripssolver:
                ej1tripsSolver = Ej1TripsSolver(self._EjTripsSolver, self._middleware)
                ej1tripsSolver.run()
            elif self._EjTripsSolver == self._ej2tripssolver:
                ej2TripsSolver = Ej2TripsSolver(self._EjTripsSolver, self._middleware)
                ej2TripsSolver.run()
            elif self._EjTripsSolver == self._ej3tripssolver:
                ej3TripsSolver = Ej3TripsSolver(self._EjTripsSolver, self._middleware)
                ej3TripsSolver.run()
            else:
                logging.error(f'action: run | result: error | EjTripsSolver: {self._EjTripsSolver} | error: Invalid filter type')
                raise Exception("Invalid filter type")
        except Exception as e:
            logging.error(f'action: run | result: error | EjTripsSolver: {self._EjTripsSolver} | error: {e}')
            if self._middleware is not None:
                self._middleware.close()
            exit(0)
