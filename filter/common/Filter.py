import logging
import os
from common.middleware import FilterMiddleware
from common.types import EOF, StationsEj3, TripsEj2, TripsEj3, WeatherEj1, StationsEj2

_EOF = "eof"

class Filter:
    def __init__(self, filter_type, filter_number, weather_ej1, stations_ej2, trips_ej2, stations_ej3, trips_ej3, middleware):
        self._sigterm = False
        self._filter_type = filter_type
        self._filter_number = filter_number
        self._weather_ej1 = weather_ej1
        self._stations_ej2 = stations_ej2
        self._trips_ej2 = trips_ej2
        self._stations_ej3 = stations_ej3
        self._trips_ej3 = trips_ej3

        self._cant_ej_trips_solver = int(os.getenv('EJTRIPSCANT', "0"))
        self._middleware: FilterMiddleware = middleware

    def _sigterm_handler(self, _signo, _stack_frame):
        self._sigterm = True
        if self._middleware is not None:
            self._middleware.close()
        exit(0)

    def run(self):
        try:
            logging.info(f'action: run | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
            if self._filter_type == self._weather_ej1:
                self._run_weather_ej1_filter()
            elif self._filter_type == self._stations_ej2:
                self._run_stations_ej2_filter()
            elif self._filter_type == self._trips_ej2:
                self._run_trips_ej2_filter()
            elif self._filter_type == self._stations_ej3:
                self._run_stations_ej3_filter()
            elif self._filter_type == self._trips_ej3:
                self._run_trips_ej3_filter()
            else:
                logging.error(f'action: run | result: error | filter_type: {self._filter_type} | filter_number: {self._filter_number} | error: Invalid filter type')
                raise Exception("Invalid filter type")
        except Exception as e:
            logging.error(f'action: run | result: error | filter_type: {self._filter_type} | filter_number: {self._filter_number} | error: {e}')
            if self._middleware is not None:
                self._middleware.close()
            exit(0)

    def _run_weather_ej1_filter(self):
        logging.info(f'action: _run_weather_ej1_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.recv_weathers_for_ej1(self._callback_weather_ej1, self._cant_ej_trips_solver)
        
    def _run_stations_ej2_filter(self):
        logging.info(f'action: _run_stations_ej2_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.recv_stations_for_ej2(self._callback_stations_ej2, self._cant_ej_trips_solver)
 
    def _run_trips_ej2_filter(self):
        logging.info(f'action: _run_trips_ej2_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.recv_trips_for_ej2(self._callback_trips_ej2)

    def _run_stations_ej3_filter(self):
        logging.info(f'action: _run_stations_ej3_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.recv_stations_for_ej3(self._callback_stations_ej3, self._cant_ej_trips_solver)

    def _run_trips_ej3_filter(self):
        logging.info(f'action: _run_trips_ej3_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.recv_trips_for_ej3(self._callback_trips_ej3)

    def _callback_weather_ej1(self, body, method=None):
        eof = self._check_eof(body, method, True)
        if eof: return
        we1 = WeatherEj1(body)
        if we1.is_valid():
            self._middleware.send_weather_ej1(we1.get_json())
        self._middleware.finished_message_processing(method)

    def _callback_stations_ej2(self, body, method=None):
        eof = self._check_eof(body, method, True)
        if eof: return
        se2 = StationsEj2(body)
        if se2.is_valid():
            self._middleware.send_stations_ej2(se2.get_json())
        self._middleware.finished_message_processing(method)

    def _callback_trips_ej2(self, body, method=None):
        eof = self._check_eof(body, method, False)
        if eof: return
        trips = body.split('\n')
        trips_for_ej2tripssolver = []
        for t in trips:
            te2 = TripsEj2(t)
            if te2.is_valid():
                trips_for_ej2tripssolver.append(te2.get_json())
        message = "\n".join(trips_for_ej2tripssolver)
        if message != "":
            self._middleware.send_trips_ej2(message)
        self._middleware.finished_message_processing(method)

    def _callback_stations_ej3(self, body, method=None):
        eof = self._check_eof(body, method, True)
        if eof: return
        se3 = StationsEj3(body)
        if se3.is_valid():
            self._middleware.send_stations_ej3(se3.get_json())
        self._middleware.finished_message_processing(method)

    def _callback_trips_ej3(self, body, method=None):
        eof = self._check_eof(body, method, False)
        if eof: return
        trips = body.split('\n')
        trips_for_ej3tripssolver = []
        for t in trips:
            te3 = TripsEj3(t)
            if te3.is_valid():
                trips_for_ej3tripssolver.append(te3.get_json())
        message = "\n".join(trips_for_ej3tripssolver)
        if message != "": 
            self._middleware.send_trips_ej3(message)
        self._middleware.finished_message_processing(method)

    def _check_eof(self, body, method, static_data):
        if (body[:3] == _EOF):
            if static_data == True:
                eof = EOF(body.split(",")[1])
                self._middleware.send_eof_static_data(eof.get_json())
            else:
                eof = self._filter_type
                self._middleware.send_eof_to_trips_listener(eof)
            logging.info(f'action: _check_eof | result: success | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
            self._middleware.finished_message_processing(method)
            self._exit()
            return True
        return False

    def _exit(self):
        self._middleware.close()
