import logging
import os
from common.middleware import EjTripsSolverMiddleware
from common.EjSolversData import Station16Or17, StationsDataForEj2, TripsForEj2

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
        station = StationsDataForEj2(body)
        if station._eof:
            finished = self._process_eof()
        elif station._type == STATIONS:
            self._stations_name[str((station._city, station._code, station._yearid))] = station._name
            self._stations[station._name] = Station16Or17()
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
            trip = TripsForEj2(t)
            if trip._eof:
                self._send_trips_to_ej2solver()
                self._middleware.finished_message_processing(method)
                self._middleware.stop_consuming()
            elif trip._type == TRIPS: 
                if str((trip._city, trip._start_station_code, trip._yearid)) not in self._stations_name:
                    continue

                station_name = self._stations_name[str((trip._city, trip._start_station_code, trip._yearid))]
                self._stations[station_name].add_trip(trip._yearid)
            else:
                logging.error(f'action: _callback_trips | result: error | EjTripsSolver: {self._ej_trips_solver} | error: Invalid type')
        self._middleware.finished_message_processing(method)
        
    def _send_trips_to_ej2solver(self):
        data = {}
        for k, v in self._stations.items():
            data[k] = str(v._trips_on_2016) + "," + str(v._trips_on_2017)
        self._middleware.send_data(str(data))
        logging.info(f'action: _send_trips_to_ej2solver | result: trips sended | EjTripsSolver: {self._ej_trips_solver}')
