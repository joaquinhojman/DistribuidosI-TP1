import json
import logging
import os
import pika

WEATHER = "weather"
TRIPS = "trips"
RESULTS = "results"

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
        finished = False
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == WEATHER:
            self._days_with_more_than_30mm_prectot[(data["city"], data["date"])] = DayWithMoreThan30mmPrectot()
        elif data["type"] == TRIPS:
            if (data["city"], data["start_date"]) in self._days_with_more_than_30mm_prectot:
                self._days_with_more_than_30mm_prectot[(data["city"], data["start_date"])].add_trip(data["duration_sec"])
        elif data["type"] == "eof":
            finished = self._process_eof(data["eof"])
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if finished: self._exit()

    def _process_eof(self, eof):
        if eof == WEATHER:
            self._weathers_eof_to_expect -= 1
            if self._weathers_eof_to_expect == 0:
                self._send_eof_confirm()
        elif eof == TRIPS:
            self._trips_eof_to_expect -= 1
            if self._trips_eof_to_expect == 0:
                self._send_results()
                return True
        else:
            logging.error(f'action: _callback | result: error | error: Invalid eof | eof: {eof}')
        return False

    def _send_eof_confirm(self):
        json_eof = json.dumps({
            "EjSolver": self._EjSolver,
            "eof": WEATHER
        })
        self._send(json_eof)

    def _send_results(self):
        json_results = json.dumps({
            "EjSolver": self._EjSolver,
            "eof": TRIPS,
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
            routing_key=RESULTS,
            body=data,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
        ))
        logging.info(f'action: _send_results | result: success')

    def _exit(self):
        self._channel.stop_consuming()
        self._channel.close()

class DayWithMoreThan30mmPrectot:
    def __init__(self):
        self._n_trips = 0
        self._total_duration = 0.0

    def add_trip(self, duration):
        self._n_trips += 1
        self._total_duration += duration

    def get_average_duration(self):
        try:
            return self._total_duration / self._n_trips
        except ZeroDivisionError:
            return 0.0
