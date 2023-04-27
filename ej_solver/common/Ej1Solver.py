import json
import logging
import os

class Ej1Solver:
    def __init__(self, EjSolver, channel):
        self._EjSolver = EjSolver
        self._channel = channel
        self._days_with_more_than_30mm_prectot = {}

    def run(self):
        logging.info(f'action: run_Ej1Solver | result: in_progress')
        self._channel.basic_consume(queue=self._EjSolver, on_message_callback=self._callback)

    def _callback(self, ch, method, properties, body):
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == "weather":
            self._days_with_more_than_30mm_prectot["date"] = DayWithMoreThan30mmPrectot()
        elif data["type"] == "trip":
            if data["start_date"] in self._days_with_more_than_30mm_prectot:
                self._days_with_more_than_30mm_prectot[data["start_date"]].add_trip(data["duration_sec"])
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')

    def _get_results(self):
        results = {}
        for key, value in self._days_with_more_than_30mm_prectot.items():
            results[key] = value.get_average_duration()
        return json.dumps(results)

class DayWithMoreThan30mmPrectot:
    def __init__(self):
        self._n_trips = 0
        self._total_duration = 0.0

    def add_trip(self, duration):
        self._n_trips += 1
        self._total_duration += duration

    def get_average_duration(self):
        return self._total_duration / self._n_trips
