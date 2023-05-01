import json
import logging
import os
import pika

EJ1TRIPS = "ej1trips"
EJ1WEATHER = "ej1weather"

class Ej1tSolver:
    def __init__(self, ejtsolver, channel):
        self._EjtSolver = ejtsolver
        self._channel = channel

        self._days_with_more_than_30mm_prectot = {}
        channel.queue_declare(queue=EJ1TRIPS, durable=True)
        channel.queue_declare(queue=EJ1WEATHER, durable=True)

    def run(self):
        logging.info(f'action: run | result: in_progress | EjtSolver: {self._EjtSolver}')
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=EJ1WEATHER, on_message_callback=self._callback_weathers)
        self._channel.start_consuming()
        logging.info(f'action: run | result: weathers getted | EjtSolver: {self._EjtSolver}')
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=self._EjtSolver, on_message_callback=self._callback_trips)
        self._channel.start_consuming()

    def _callback_weathers(self, ch, method, properties, body):
        body = body.decode("utf-8")
        days_list = eval(body)
        for day in days_list:
            self._days_with_more_than_30mm_prectot[day] = DayWithMoreThan30mmPrectot()
        ch.basic_ack(delivery_tag=method.delivery_tag)
        self._channel.stop_consuming()

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        data = json.loads(body)
        if data["type"] == "trips":
            key = str((data["city"], data["start_date"]))
            if key in self._days_with_more_than_30mm_prectot:
                self._days_with_more_than_30mm_prectot[key].add_trip(data["duration_sec"])
        elif data["type"] == "eof":
            self._send_trips_to_ej1solver()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._channel.stop_consuming()
            return
        else:
            logging.error(f'action: _callback_trips | result: error | EjtSolver: {self._EjtSolver} | error: Invalid type')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def _send_trips_to_ej1solver(self):
        data = {}
        for k, v in self._days_with_more_than_30mm_prectot.items():
            data[k] = str(v._n_trips) + "," + str(v._total_duration)
        self._channel.basic_publish(exchange='', routing_key=EJ1TRIPS, body=str(data))
        logging.info(f'action: _send_trips_to_ej1solver | result: trips sended | EjtSolver: {self._EjtSolver}')

class DayWithMoreThan30mmPrectot:
    def __init__(self):
        self._n_trips = 0
        self._total_duration = 0.0

    def add_trip(self, duration):
        self._n_trips += 1
        self._total_duration += duration
