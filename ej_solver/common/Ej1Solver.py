import json
import logging
import os
import pika

class Ej1Solver:
    def __init__(self, EjSolver, channel):
        self._EjSolver = EjSolver
        self._channel = channel
        
        self._weathers_eof_to_expect = int(os.getenv('WE1FCANT', ""))
        self._trips_eof_to_expect = int(os.getenv('TBRKCANT', ""))

        self._days_with_more_than_30mm_prectot = {}

    def run(self):
        logging.info(f'action: run_Ej1Solver | result: in_progress')
        self._channel.basic_consume(queue=self._EjSolver, on_message_callback=self._callback)

    def _callback(self, ch, method, properties, body):
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == "weather":
            self._days_with_more_than_30mm_prectot[(data["city"], data["date"])] = DayWithMoreThan30mmPrectot()
        elif data["type"] == "trip":
            if (data["city"],data["start_date"]) in self._days_with_more_than_30mm_prectot:
                self._days_with_more_than_30mm_prectot[(data["city"], data["start_date"])].add_trip(data["duration_sec"])
        elif data["type"] == "eof":
            self._process_eof(data["eof"])
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')

    def _process_eof(self, eof):
        if eof == "weather":
            self._weathers_eof_to_expect -= 1
            if self._weathers_eof_to_expect == 0:
                self._send_eof_confirm()
        elif eof == "trip":
            self._trips_eof_to_expect -= 1
            if self._trips_eof_to_expect == 0:
                self._send_results()
                self._exit()
        else:
            logging.error(f'action: _callback | result: error | error: Invalid eof | eof: {eof}')

    def _send_eof_confirm(self, eof):
        json_eof = json.dumps({
            "EjSolver": self._EjSolver,
            "eof": "weather"
        })
        self._send(json_eof)

    def _send_results(self):
        json_results = json.dumps({
            "EjSolver": self._EjSolver,
            "eof": "trip",
            "results": str(self._get_results())
        })
        self._send(json_results)

    def _get_results(self):
        results = {}
        for key, value in self._days_with_more_than_30mm_prectot.items():
            results[key] = value.get_average_duration()
        return results
    
    def _send(self, data):
        self._channel.basic_publish(
            exchange='',
            routing_key='results',
            body=data,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
        ))
        logging.info(f'action: _send | result: success | data: {data}')

    def _exit(self):
        self._channel.stop_consuming()
        exit(0)

class DayWithMoreThan30mmPrectot:
    def __init__(self):
        self._n_trips = 0
        self._total_duration = 0.0

    def add_trip(self, duration):
        self._n_trips += 1
        self._total_duration += duration

    def get_average_duration(self):
        return self._total_duration / self._n_trips
