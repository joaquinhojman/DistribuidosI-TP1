
import os
import logging
from common.middleware import EofListenerMiddleware

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
    def __init__(self, middleware):
        self._sigterm = False
        self._middleware: EofListenerMiddleware = middleware

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
        finished = self._proccess_eof(body)
        self._middleware.finished_message_processing(method)
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
                self._middleware.send_eof_to_filter(WEATHEREJ1FILTER, EOF + "," + body)
        elif body == STATIONS:
            for _ in range(self._cant_filters[STATIONSEJ2FILTER]):
                self._middleware.send_eof_to_filter(STATIONSEJ2FILTER, EOF + "," + body)
            for _ in range(self._cant_filters[STATIONSEJ3FILTER]):
                self._middleware.send_eof_to_filter(STATIONSEJ3FILTER, EOF + "," + body)
        elif body == TRIPS:
            for _ in range(self._cant_filters[TRIPSEJ2FILTER]):
                self._middleware.send_eof_to_filter(TRIPSEJ2FILTER, EOF + "," + body)
            for _ in range(self._cant_filters[TRIPSEJ3FILTER]):
                self._middleware.send_eof_to_filter(TRIPSEJ3FILTER, EOF + "," + body)
            return True
        else:
            logging.error(f'action: send eof | result: error | error: invalid body')
            return
        logging.info(f'action: send eof | result: success | body: {body}')
        return False

    def _exit(self):
        self._middleware.close()
