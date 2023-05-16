import logging
from common.middleware import EjTripsSolverMiddleware

from common.Ej1TripsSolver import Ej1TripsSolver
from common.Ej2TripsSolver import Ej2TripsSolver
from common.Ej3TripsSolver import Ej3TripsSolver

class EjTripsSolver:
    def __init__(self, ejtripssolver, id, ej1tripssolver, ej2tripssolver, ej3tripssolver, middleware):
        self._sigterm = False
        self._ej_trips_solver = ejtripssolver
        self._ej1_trips_solver = ej1tripssolver
        self._ej2_trips_solver = ej2tripssolver
        self._ej3_trips_solver = ej3tripssolver
        self._id = id

        self._middleware: EjTripsSolverMiddleware = middleware

    def _sigterm_handler(self, _signo, _stack_frame):
        self._sigterm = True
        if self._middleware is not None:
            self._middleware.close()
        exit(0)

    def run(self):
        try:
            logging.info(f'action: run | result: in_progress | EjSolver: {self._ej_trips_solver}')
            if self._ej_trips_solver == self._ej1_trips_solver:
                ej1tripsSolver = Ej1TripsSolver(self._ej_trips_solver, self._id, self._middleware)
                ej1tripsSolver.run()
            elif self._ej_trips_solver == self._ej2_trips_solver:
                ej2TripsSolver = Ej2TripsSolver(self._ej_trips_solver, self._id, self._middleware)
                ej2TripsSolver.run()
            elif self._ej_trips_solver == self._ej3_trips_solver:
                ej3TripsSolver = Ej3TripsSolver(self._ej_trips_solver, self._id, self._middleware)
                ej3TripsSolver.run()
            else:
                logging.error(f'action: run | result: error | EjTripsSolver: {self._ej_trips_solver} | error: Invalid filter type')
                raise Exception("Invalid filter type")
        except Exception as e:
            logging.error(f'action: run | result: error | EjTripsSolver: {self._ej_trips_solver} | error: {e}')
            if self._middleware is not None:
                self._middleware.close()
            exit(0)
