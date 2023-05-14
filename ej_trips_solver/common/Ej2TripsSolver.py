import json
import logging
import os
from common.middleware import EjTripsSolverMiddleware

STATIONS = "stations"
TRIPS = "trips"
EOF = "eof"

class Ej2TripsSolver:
    def __init__(self, ejTripssolver, id, middleware):
        self._ej_trips_solver = ejTripssolver
        self._id = id
        self._stations_eof_to_expect = int(os.getenv('SE2FCANT', ""))

        self._middleware: EjTripsSolverMiddleware = middleware
        self._stations_name = {}
        self._stations = {}
        self._stations_queue = None

    def run(self):
        logging.info(f'action: run | result: in_progress | EjTripsSolver: {self._ej_trips_solver}')
        self._middleware.recv_static_data(callback=self._callback_stations)
        logging.info(f'action: run | result: weathers getted | EjTripsSolver: {self._ej_trips_solver}')
        self._middleware.recv_trips(callback=self._callback_trips)

    def _callback_stations(self, body, method=None):
        finished = False
        station = StationData(body)
        if station._eof:
            finished = self._process_eof()
        elif station._type == STATIONS:
            self._stations_name[str((station._city, station._code, station._yearid))] = station._name
            self._stations[station._name] = Station()
            self._middleware.send_data(body)
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {station}')
        self._middleware.finished_message_processing(method)
        if finished:
            self._middleware.send_data(body)
            self._middleware.stop_consuming()
    
    def _process_eof(self):
        self._stations_eof_to_expect -= 1
        if self._stations_eof_to_expect == 0:
            return True
        return False

    def _callback_trips(self, body, method=None):
        trips = body.split("\n")
        for t in trips:
            data = json.loads(t)
            if data["type"] == TRIPS: 
                if str((data["city"], data["start_station_code"], data["yearid"])) not in self._stations_name:
                    continue

                station_name = self._stations_name[str((data["city"], data["start_station_code"], data["yearid"]))]
                self._stations[station_name].add_trip(data["yearid"])
            elif data["type"] == EOF:
                self._send_trips_to_ej2solver()
                self._middleware.finished_message_processing(method)
                self._middleware.stop_consuming()
                return
            else:
                logging.error(f'action: _callback_trips | result: error | EjTripsSolver: {self._ej_trips_solver} | error: Invalid type')
        self._middleware.finished_message_processing(method)
        
    def _send_trips_to_ej2solver(self):
        data = {}
        for k, v in self._stations.items():
            data[k] = str(v._trips_on_2016) + "," + str(v._trips_on_2017)
        self._middleware.send_data(str(data))
        logging.info(f'action: _send_trips_to_ej2solver | result: trips sended | EjTripsSolver: {self._ej_trips_solver}')

class StationData:
    def __init__(self, body):
        data = json.loads(body)
        self._type = data["type"]
        self._eof = True if self._type == EOF else False
        self._city = None
        self._code = None
        self._yearid = None
        self._name = None
        if self._eof == False:
            self._city = data["city"]
            self._code = data["code"]
            self._yearid = data["yearid"]
            self._name = data["name"]

class Station:
    def __init__(self):
        self._trips_on_2016 = 0
        self._trips_on_2017 = 0
    
    def add_trip(self, year):
        if year == "2016":
            self._trips_on_2016 += 1
        elif year == "2017":
            self._trips_on_2017 += 1
        else:
            logging.error(f'action: add_trip | result: error | error: Invalid year | year: {year}')
