import json
import logging
import os
from common.middleware import EjSolverMiddleware
from common.EjSolversData import Station16Or17, StationsDataForEj2

STATIONS = "stations"
TRIPS = "trips"
RESULTS = "results"
EOF = "eof"
YEAR_2016 = "2016"
YEAR_2017 = "2017"

class Ej2Solver:
    def __init__(self, EjSolver, middleware):
        self._EjSolver = EjSolver
        self._middleware: EjSolverMiddleware = middleware

        self._stations_eof_to_expect = int(os.getenv('EJ2TCANT', ""))
        self._ej2_trips_solvers_cant = int(os.getenv('EJ2TCANT', ""))

        self._stations_name = {}
        self._stations = {}

    def run(self):
        logging.info(f'action: run_Ej2Solver | result: in_progress')
        self._middleware.recv_data(callback=self._callback_stations)
        self._middleware.recv_data(callback=self._callback_trips)

    def _callback_stations(self, body, method=None):
        finished = False
        station = StationsDataForEj2(body)
        if station._eof:
            finished = self._process_eof()
        elif station._type == STATIONS:
            if str((station._city, station._code, station._yearid)) not in self._stations_name:
                self._stations_name[str((station._city, station._code, station._yearid))] = station._name
                self._stations[station._name] = Station16Or17()
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
        self._ej2_trips_solvers_cant -= 1
        trips = eval(body)
        for k, v in trips.items():
            values = v.split(",")
            self._stations[k].add_trips(YEAR_2016, int(values[0]))
            self._stations[k].add_trips(YEAR_2017, int(values[1]))
        self._middleware.finished_message_processing(method)
        if self._ej2_trips_solvers_cant == 0:
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
        for key, value in self._stations.items():
            if value.duplicate_trips():
                results[key] = (value._trips_on_2016, value._trips_on_2017)
        return results
    
    def _exit(self):
        self._middleware.close()
