import json
import logging
import os
from common.Middleware import Middleware

WEATHER = "weather"
TRIPS = "trips"
RESULTS = "results"
EOF = "eof"

class Ej1Solver:
    def __init__(self, EjSolver, middleware):
        self._EjSolver = EjSolver
        self._middleware: Middleware = middleware
        
        self._weathers_eof_to_expect = int(os.getenv('EJ1TCANT', ""))
        self._ej1tripssolvers_cant = int(os.getenv('EJ1TCANT', ""))

        self._days_with_more_than_30mm_prectot = {}

    def run(self):
        logging.info(f'action: run_Ej1Solver | result: in_progress')
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
        if data["type"] == WEATHER:
            if str((data["city"], data["date"])) not in self._days_with_more_than_30mm_prectot:
                self._days_with_more_than_30mm_prectot[str((data["city"], data["date"]))] = DayWithMoreThan30mmPrectot()
        elif data["type"] == EOF:
            finished = self._process_eof()
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
        self._middleware.send_ack(method.delivery_tag)
        if finished: self._middleware.stop_consuming()

    def _process_eof(self,):
        self._weathers_eof_to_expect -= 1
        if self._weathers_eof_to_expect == 0:
            self._send_eof_confirm()
            return True
        return False

    def _send_eof_confirm(self):
        json_eof = json.dumps({
            "EjSolver": self._EjSolver,
            EOF: WEATHER
        })
        self._send(json_eof)
    
    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        self._ej1tripssolvers_cant -= 1
        trips = eval(body)
        for k, v in trips.items():
            values = v.split(",")
            self._days_with_more_than_30mm_prectot[k].add_trips(int(values[0]), float(values[1]))
        self._middleware.send_ack(method.delivery_tag)
        if self._ej1tripssolvers_cant == 0:
            self._send_results()
            self._exit()

    def _send_results(self):
        json_results = json.dumps({
            "EjSolver": self._EjSolver,
            EOF: TRIPS,
            "results": str(self._get_results())
        })
        self._send(json_results)

    def _get_results(self):
        results = {}
        for key, value in self._days_with_more_than_30mm_prectot.items():
            average_duration = value.get_average_duration()
            if average_duration > 0.0:
                results[key] = value.get_average_duration()
        return results
    
    def _send(self, data):
        self._middleware.send_message(queue=RESULTS, data=data)
        logging.info(f'action: _send_results | result: success')

    def _exit(self):
        self._middleware.stop_consuming()
        self._middleware.close()

class DayWithMoreThan30mmPrectot:
    def __init__(self):
        self._n_trips = 0
        self._total_duration = 0.0

    def add_trips(self, n, duration):
        self._n_trips += n
        self._total_duration += duration

    def get_average_duration(self):
        try:
            return self._total_duration / self._n_trips
        except ZeroDivisionError:
            return 0.0
