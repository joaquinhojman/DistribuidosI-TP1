import json
import logging
import os
from common.middleware import EjTripsSolverMiddleware

WEATHER = "weather"
TRIPS = "trips"
EOF = "eof"

class Ej1TripsSolver:
    def __init__(self, ejtripssolver, id, middleware):
        self._ej_trips_solver = ejtripssolver
        self._id = id
        self._weathers_eof_to_expect = int(os.getenv('WE1FCANT', ""))

        self._middleware: EjTripsSolverMiddleware = middleware
        self._days_with_more_than_30mm_prectot = {}

    def run(self):
        logging.info(f'action: run | result: in_progress | EjTripsSolver: {self._ej_trips_solver}')
        self._middleware.recv_static_data(callback=self._callback_weathers)
        logging.info(f'action: run | result: weathers getted | EjTripsSolver: {self._ej_trips_solver}')
        self._middleware.recv_trips(callback=self._callback_trips)

    def _callback_weathers(self, body, method=None):
        finished = False
        weather = Weather(body)
        if weather._eof:
            finished = self._process_eof()
        elif weather._type == WEATHER:
            self._days_with_more_than_30mm_prectot[str((weather._city, weather._date))] = DayWithMoreThan30mmPrectot()
            self._middleware.send_data(body)
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {weather}')
        self._middleware.finished_message_processing(method)
        if finished: 
            self._middleware.send_data(body)
            self._middleware.stop_consuming()

    def _process_eof(self,):
        self._weathers_eof_to_expect -= 1
        if self._weathers_eof_to_expect == 0:
            return True
        return False

    def _callback_trips(self, body, method=None):
        trips = body.split("\n")
        for t in trips:
            data = json.loads(t)
            if data["type"] == TRIPS:
                key = str((data["city"], data["start_date"]))
                if key in self._days_with_more_than_30mm_prectot:
                    self._days_with_more_than_30mm_prectot[key].add_trip(float(data["duration_sec"]))
            elif data["type"] == EOF:
                self._send_trips_to_ej1solver()
                self._middleware.finished_message_processing(method)
                self._middleware.stop_consuming()
                return
            else:
                logging.error(f'action: _callback_trips | result: error | EjTripsSolver: {self._ETtripsSgolver} | error: Invalid type')
        self._middleware.finished_message_processing(method)
        
    def _send_trips_to_ej1solver(self):
        data = {}
        for k, v in self._days_with_more_than_30mm_prectot.items():
            data[k] = str(v._n_trips) + "," + str(v._total_duration)
        self._middleware.send_data(str(data))
        logging.info(f'action: _send_trips_to_ej1solver | result: trips sended | EjTripsSolver: {self._ej_trips_solver}')

class Weather:
    def __init__(self, body):
        data = json.loads(body)
        self._type = data["type"]
        self._eof = True if self._type == EOF else False
        self._city = None
        self._date = None
        if self._eof == False:
            self._city = data["city"]
            self._date = data["date"]

class DayWithMoreThan30mmPrectot:
    def __init__(self):
        self._n_trips = 0
        self._total_duration = 0.0

    def add_trip(self, duration):
        self._n_trips += 1
        self._total_duration += duration
