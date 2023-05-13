
import json
import os
import logging
from common.middleware import EofTripsListenerMiddleware

TRIPS = "trips"
TRIPSEJ2 = "tripsej2"
TRIPSEJ3 = "tripsej3"
EJ1TRIPSSOLVER = "ej1tripssolver"
EJ2TRIPSSOLVER = "ej2tripssolver"
EJ3TRIPSSOLVER = "ej3tripssolver"

class EofTripsListener:
    def __init__(self, middleware):
        self._sigterm = False
        self._middleware: EofTripsListenerMiddleware = middleware

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

    def _sigterm_handler(self, _signo, _stack_frame):
        self._sigterm = True
        if self._middleware is not None:
            self._middleware.close()
        exit(0)

    def run(self):
        try:
            logging.info(f'action: run | result: in_progress')
            self._middleware.recv_eofs(self._callback)
        except Exception as e:
            logging.error(f'action: run | result: error | error: {e}')
            if self._middleware is not None:
                self._middleware.close()
            exit(0)

    def _callback(self, body, method=None):
        self._proccess_eof(body)
        self._middleware.finished_message_processing(method)
        if self._finished(): self._exit()

    def _proccess_eof(self, body):
        self._remaining_eof[body] -= 1
        logging.info(f'action: proccess eof | result: in_progress | body: {body} | remaining: {self._remaining_eof[body]}')
        if self._remaining_eof[body] == 0:
            self._send_eofs(body)

    def _send_eofs(self, body):
        if body == TRIPSEJ2:
            for _ in range(self._cant_solvers[EJ2TRIPSSOLVER]):
                self._middleware.send_eof_to_ej_trips_solver(EJ2TRIPSSOLVER, self._get_eof())
        elif body == TRIPSEJ3:
            for _ in range(self._cant_solvers[EJ3TRIPSSOLVER]):
                self._middleware.send_eof_to_ej_trips_solver(EJ3TRIPSSOLVER, self._get_eof())
        elif body == TRIPS:
            for _ in range(self._cant_solvers[EJ1TRIPSSOLVER]):
                self._middleware.send_eof_to_ej_trips_solver(EJ1TRIPSSOLVER, self._get_eof())
        else:
            logging.error(f'action: send eof | result: error | error: invalid body')
            return
        logging.info(f'action: send eof | result: success | body: {body}')

    def _finished(self):
        return self._remaining_eof[TRIPSEJ2] == 0 and self._remaining_eof[TRIPSEJ3] == 0 and self._remaining_eof[TRIPS] == 0

    def _get_eof(self):
        return json.dumps({
            "type": "eof",
        })

    def _exit(self):
        self._middleware.close()
