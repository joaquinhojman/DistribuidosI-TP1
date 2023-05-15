import json
import logging
import os
from common.middleware import EjSolverMiddleware
from common.EjSolversData import MontrealStation, StationsDataForEj3

STATIONS = "stations"
TRIPS = "trips"
RESULTS = "results"
EOF = "eof"

class Ej3Solver:
    def __init__(self, EjSolver, middleware):
        self._EjSolver = EjSolver
        self._middleware: EjSolverMiddleware = middleware
        
        self._stations_eof_to_expect = int(os.getenv('EJ3TCANT', ""))
        self._ej3_trips_solvers_cant = int(os.getenv('EJ3TCANT', ""))

        self._stations_name = {}
        self._montreal_stations = {}

    def run(self):
        logging.info(f'action: run_Ej3Solver | result: in_progress')
        self._middleware.recv_data(callback=self._callback_stations)
        self._middleware.recv_data(callback=self._callback_trips)

    def _callback_stations(self, body, method=None):
        finished = False
        station = StationsDataForEj3(body)
        if station._eof:
            finished = self._process_eof()
        elif station._type == STATIONS:
            if str((station._code, station._yearid)) not in self._stations_name:
                self._stations_name[str((station._code, station._yearid))] = station._name
                self._montreal_stations[station._name] = MontrealStation(station._latitude, station._longitude)
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {station}')
        self._middleware.finished_message_processing(method)
        if finished: self._middleware.stop_consuming()
    
    def _process_eof(self):
        self._stations_eof_to_expect -= 1
        if self._stations_eof_to_expect == 0:
            self._send_eof_confirm()
            return True
        return False
        
    def _send_eof_confirm(self):
        json_eof = json.dumps({
            "EjSolver": self._EjSolver,
            EOF: STATIONS
        })
        self._middleware.send_data(data=json_eof)

    def _callback_trips(self, body, method=None):
        self._ej3_trips_solvers_cant -= 1
        trips = eval(body)
        for k, v in trips.items():
            values = v.split(",")
            self._montreal_stations[k].add_trips(int(values[0]), float(values[1]))
        self._middleware.finished_message_processing(method)
        if self._ej3_trips_solvers_cant == 0:
            self._send_results()
            self._exit()

    def _send_results(self):
        results = self._get_results()
        json_results = json.dumps({
            "EjSolver": self._EjSolver,
            EOF: TRIPS,
            "results": str(results)
        })
        self._middleware.send_data(data=json_results)

    def _get_results(self):
        results = {}
        for key, value in self._montreal_stations.items():
            avg_km = value.get_average_km()
            if avg_km > 6.0:
                results[key] = avg_km
        return results

    def _exit(self):
        self._middleware.close()
