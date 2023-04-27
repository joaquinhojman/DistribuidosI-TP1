import json
import logging
import os
from time import sleep
import pika

class Ej2Solver:
    def __init__(self, EjSolver, channel):
        self._EjSolver = EjSolver
        self._channel = channel
        self._stations = {}

    def run(self):
        logging.info(f'action: run_Ej2Solver | result: in_progress')
        self._channel.basic_consume(queue=self._EjSolver, on_message_callback=self._callback)

    def _callback(self, ch, method, properties, body):
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == "station":
            self._stations[data["code"]] = Station(data["name"])
        elif data["type"] == "trip":
            if data["start_date_year"] == "2016":
                self._stations[data["start_station_code"]].add_2016_trip()
            elif data["start_date_year"] == "2017":
                self._stations[data["start_station_code"]].add_2017_trip()
            else:
                logging.error(f'action: _callback | result: error | error: Invalid year | data: {data}')
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')

    def _get_results(self):
        results = []
        for _key, value in self._stations.items():
            if value.duplicate_trips():
                results.append(value._name)
        return str(results)

class Station:
    def __init__(self, name):
        self._name = name
        self._trips_on_2016 = 0
        self._trips_on_2017 = 0

    def add_2016_trip(self):
        self._trips_on_2016 += 1
    
    def add_2017_trip(self):
        self._trips_on_2017 += 1

    def duplicate_trips(self):
        return self.add_2016_trip * 2 <= self.add_2017_trip
