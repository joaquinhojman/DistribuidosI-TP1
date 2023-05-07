import json
import logging
import os
from common.Middleware import Middleware

STATIONS = "stations"
TRIPS = "trips"
RESULTS = "results"

class Ej2Solver:
    def __init__(self, EjSolver, middleware):
        self._EjSolver = EjSolver
        self._middleware: Middleware = middleware

        self._stations_eof_to_expect = int(os.getenv('EJ2TCANT', ""))
        self._ej2tsolvers_cant = int(os.getenv('EJ2TCANT', ""))

        self._stations_name = {}
        self._stations = {}

    def run(self):
        logging.info(f'action: run_Ej2Solver | result: in_progress')
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
            if str((data["city"], data["code"], data["yearid"])) not in self._stations_name:
                self._stations_name[str((data["city"], data["code"], data["yearid"]))] = data["name"]
                self._stations[data["name"]] = Station()
        elif data["type"] == "eof":
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
            "eof": STATIONS
        })
        self._send(json_eof)

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        self._ej2tsolvers_cant -= 1
        trips = eval(body)
        for k, v in trips.items():
            values = v.split(",")
            self._stations[k].add_trip("2016", int(values[0]))
            self._stations[k].add_trip("2017", int(values[1]))
        self._middleware.send_ack(method.delivery_tag)
        if self._ej2tsolvers_cant == 0:
            self._send_results()
            self._exit()

    def _send_results(self):
        results = self._get_results()
        json_results = json.dumps({
            "EjSolver": self._EjSolver,
            "eof": TRIPS,
            "results": str(results)
        })
        self._send(json_results)

    def _get_results(self):
        results = {}
        for key, value in self._stations.items():
            if value.duplicate_trips():
                results[key] = (value._trips_on_2016, value._trips_on_2017)
        return results
    
    def _send(self, data):
        self._middleware.send_message(queue=RESULTS, data=data)
        logging.info(f'action: _send_results | result: success')

    def _exit(self):
        self._middleware.stop_consuming()
        self._middleware.close()

class Station:
    def __init__(self):
        self._trips_on_2016 = 0
        self._trips_on_2017 = 0
    
    def add_trip(self, year, n):
        if year == "2016":
            self._trips_on_2016 += n
        elif year == "2017":
            self._trips_on_2017 += n
        else:
            logging.error(f'action: add_trip | result: error | error: Invalid year | year: {year}')

    def duplicate_trips(self):
        return (self._trips_on_2016 * 2 <= self._trips_on_2017) and self._trips_on_2017 != 0
