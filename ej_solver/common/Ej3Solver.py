import json
import logging
import os
from common.middleware import EjSolverMiddleware

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
        station = MontrealStationData(body)
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
            self._montreal_stations[k].add_trip(int(values[0]), float(values[1]))
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


class MontrealStationData:
    def __init__(self, body):
        data = json.loads(body)
        self._type = data["type"]
        self._eof = True if self._type == EOF else False
        self._code = None
        self._yearid = None
        self._name = None
        self._latitude = None
        self._longitude = None
        if self._eof == False:
            self._code = data["code"]
            self._yearid = data["yearid"]
            self._name = data["name"]
            self._latitude = data["latitude"]
            self._longitude = data["longitude"]

class MontrealStation:
    def __init__(self, latitude, longitude):
        self._latitude = float(latitude)
        self._longitude = float(longitude)

        self._trips = 0
        self._total_km_to_come = 0.0

    def add_trip(self, n, km):
        self._trips += n
        self._total_km_to_come += km
    
    def get_average_km(self):
        try:
            return self._total_km_to_come / self._trips
        except ZeroDivisionError:
            return 0.0
