import json
import logging
import os
from common.Middleware import Middleware

EJ1SOLVER = "ej1solver"
WEATHER = "weather"
TRIPS = "trips"
WE1FILTER = "we1"
WEATHER_EJ1_EXCHANGE = "weather_ej1_exchange"
EOF = "eof"

class Ej1tSolver:
    def __init__(self, ejtsolver, middleware):
        self._EjtSolver = ejtsolver
        self._id = os.getenv('EJ1TSOLVER_ID', "")
        self._weathers_eof_to_expect = int(os.getenv('WE1FCANT', ""))

        self._middleware: Middleware = middleware
        self._days_with_more_than_30mm_prectot = {}
        self._wheater_queue = None

        self._initialize_rabbitmq()

    def _initialize_rabbitmq(self):
        self._middleware.exchange_declare(exchange=WEATHER_EJ1_EXCHANGE, exchange_type='fanout')

        self._wheater_queue = f'{WE1FILTER}_{self._id}'
        self._middleware.queue_declare(queue=self._wheater_queue, durable=True)
        self._middleware.queue_bind(exchange=WEATHER_EJ1_EXCHANGE, queue=self._wheater_queue)
        
        self._middleware.queue_declare(queue=EJ1SOLVER, durable=True)

    def run(self):
        logging.info(f'action: run | result: in_progress | EjtSolver: {self._EjtSolver}')
        self._middleware.basic_qos(prefetch_count=1)
        self._middleware.recv_message(queue=self._wheater_queue, callback=self._callback_weathers)
        self._middleware.start_consuming()
        logging.info(f'action: run | result: weathers getted | EjtSolver: {self._EjtSolver}')
        self._middleware.basic_qos(prefetch_count=1)
        self._middleware.recv_message(queue=self._EjtSolver, callback=self._callback_trips)
        self._middleware.start_consuming()

    def _callback_weathers(self, ch, method, properties, body):
        finished = False
        body = body.decode("utf-8")
        data = json.loads(body)
        if data["type"] == WEATHER:
            self._days_with_more_than_30mm_prectot[str((data["city"], data["date"]))] = DayWithMoreThan30mmPrectot()
            self._middleware.send_message(queue=EJ1SOLVER, data=body)
        elif data["type"] == EOF:
            finished = self._process_eof()
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
        self._middleware.send_ack(method.delivery_tag)
        if finished: 
            self._middleware.send_message(queue=EJ1SOLVER, data=body)
            self._middleware.stop_consuming()

    def _process_eof(self,):
        self._weathers_eof_to_expect -= 1
        if self._weathers_eof_to_expect == 0:
            return True
        return False

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        data = json.loads(body)
        if data["type"] == TRIPS:
            key = str((data["city"], data["start_date"]))
            if key in self._days_with_more_than_30mm_prectot:
                self._days_with_more_than_30mm_prectot[key].add_trip(float(data["duration_sec"]))
        elif data["type"] == EOF:
            self._send_trips_to_ej1solver()
            self._middleware.send_ack(method.delivery_tag)
            self._middleware.stop_consuming()
            return
        else:
            logging.error(f'action: _callback_trips | result: error | EjtSolver: {self._EjtSolver} | error: Invalid type')
        self._middleware.send_ack(method.delivery_tag)
        
    def _send_trips_to_ej1solver(self):
        data = {}
        for k, v in self._days_with_more_than_30mm_prectot.items():
            data[k] = str(v._n_trips) + "," + str(v._total_duration)
        self._middleware.send_message(queue=EJ1SOLVER, data=str(data))
        logging.info(f'action: _send_trips_to_ej1solver | result: trips sended | EjtSolver: {self._EjtSolver}')

class DayWithMoreThan30mmPrectot:
    def __init__(self):
        self._n_trips = 0
        self._total_duration = 0.0

    def add_trip(self, duration):
        self._n_trips += 1
        self._total_duration += duration
