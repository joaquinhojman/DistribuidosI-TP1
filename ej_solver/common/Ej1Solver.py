import json
import logging
import os
import pika

WEATHER = "weather"
TRIPS = "trips"
EJ1TRIPS = "ej1trips"
EJ1WEATHER = "ej1weather"
RESULTS = "results"

class Ej1Solver:
    def __init__(self, EjSolver, channel):
        self._EjSolver = EjSolver
        self._channel = channel
        
        self._weathers_eof_to_expect = int(os.getenv('WE1FCANT', ""))
        self._ej1tsolvers_cant = int(os.getenv('EJ1TCANT', ""))

        self._days_with_more_than_30mm_prectot = {}
        channel.queue_declare(queue=EJ1TRIPS, durable=True)
        channel.queue_declare(queue=EJ1WEATHER, durable=True)

    def run(self):
        logging.info(f'action: run_Ej1Solver | result: in_progress')
        self._channel.basic_consume(queue=self._EjSolver, on_message_callback=self._callback)
        self._channel.basic_consume(queue=EJ1TRIPS, on_message_callback=self._callback_trips)

    def _callback(self, ch, method, properties, body):
        finished = False
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == WEATHER:
            self._days_with_more_than_30mm_prectot[str((data["city"], data["date"]))] = DayWithMoreThan30mmPrectot()
        elif data["type"] == "eof":
            finished = self._process_eof()
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if finished: self._channel.stop_consuming()

    def _process_eof(self,):
        self._weathers_eof_to_expect -= 1
        if self._weathers_eof_to_expect == 0:
            self._send_weather_to_ejt1solver()
            self._send_eof_confirm()
            return True
        return False

    def _send_weather_to_ejt1solver(self):
        data = str(self._days_with_more_than_30mm_prectot.keys())
        for _ in range(0, self._ej1tsolvers_cant):
            self._channel.basic_publish(
                exchange='',
                routing_key=EJ1WEATHER,
                body=data,
                properties=pika.BasicProperties(
                delivery_mode = 2, # make message persistent
            ))

    def _send_eof_confirm(self):
        json_eof = json.dumps({
            "EjSolver": self._EjSolver,
            "eof": WEATHER
        })
        self._send(json_eof)
    
    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        self._ej1tsolvers_cant -= 1
        trips = eval(body)
        for k, v in trips.items():
            self._days_with_more_than_30mm_prectot[k].add_trip(v[0], v[1])
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if self._ej1tsolvers_cant == 0:
            self._send_results()
            self._exit()

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
            average_duration = value.get_average_duration()
            if average_duration > 0.0:
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

    def add_trips(self, n, duration):
        self._n_trips += n
        self._total_duration += duration

    def get_average_duration(self):
        try:
            return self._total_duration / self._n_trips
        except ZeroDivisionError:
            return 0.0
