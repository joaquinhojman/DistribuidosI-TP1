import json
import logging
import os
from common.Middleware import Middleware

STATIONS = "stations"
TRIPS = "trips"
RESULTS = "results"
EOF = "eof"

class Ej3Solver:
    def __init__(self, EjSolver, middleware):
        self._EjSolver = EjSolver
        self._middleware: Middleware = middleware
        
        self._stations_eof_to_expect = int(os.getenv('EJ3TCANT', ""))
        self._ej3tripssolvers_cant = int(os.getenv('EJ3TCANT', ""))

        self._stations_name = {}
        self._montreal_stations = {}

    def run(self):
        logging.info(f'action: run_Ej3Solver | result: in_progress')
        self._middleware.basic_qos(prefetch_count=1)
        self._middleware.recv_message(queue=self._EjSolver, callback=self._callback)
        self._middleware.start_consuming()
        self._middleware.basic_qos(prefetch_count=1)
        self._middleware.recv_message(queue=self._EjSolver, callback=self._callback_trips)
        self._middleware.start_consuming()

    def _callback(self, ch, method, properties, body):
        finished = False
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == STATIONS:
            if str((data["code"], data["yearid"])) not in self._stations_name:
                self._stations_name[str((data["code"], data["yearid"]))] = data["name"]
                self._montreal_stations[data["name"]] = MontrealStation(data["latitude"], data["longitude"])
        elif data["type"] == EOF:
            finished = self._process_eof()
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
        self._middleware.send_ack(method.delivery_tag)
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
        self._send(json_eof)

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        self._ej3tripssolvers_cant -= 1
        trips = eval(body)
        for k, v in trips.items():
            values = v.split(",")
            self._montreal_stations[k].add_trip(int(values[0]), float(values[1]))
        self._middleware.send_ack(method.delivery_tag)
        if self._ej3tripssolvers_cant == 0:
            self._send_results()
            self._exit()

    def _send_results(self):
        results = self._get_results()
        json_results = json.dumps({
            "EjSolver": self._EjSolver,
            EOF: TRIPS,
            "results": str(results)
        })
        self._send(json_results)

    def _get_results(self):
        results = {}
        for key, value in self._montreal_stations.items():
            avg_km = value.get_average_km()
            if avg_km > 6.0:
                results[key] = avg_km
        return results

    def _send(self, data):
        self._middleware.send_message(queue=RESULTS, data=data)
        logging.info(f'action: _send_results | result: success')

    def _exit(self):
        self._middleware.stop_consuming()
        self._middleware.close()

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
