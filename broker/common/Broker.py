import logging
import os
from time import sleep
import pika

from common.types import Station, Trip, Weather

class Broker:
    def __init__(self, broker_type, broker_number, weather, stations, trips):
        self._broker_type = broker_type
        self._broker_number = broker_number
        self._weather = weather
        self._stations = stations
        self._trips = trips

        self._channel = None
        self._initialize_rabbitmq()

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        logging.info(f'action: Handle SIGTERM | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number}')

    def _initialize_rabbitmq(self):
        logging.info(f'action: initialize_rabbitmq | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        while self._channel is None:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq'))
                channel = connection.channel()

                channel.queue_declare(queue=self._broker_type, durable=True)
                self._channel = channel
            except Exception as e:
                sleep(5)
        logging.info(f'action: initialize_rabbitmq | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number}')

    def run(self):
        logging.info(f'action: run | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        self._channel.basic_qos(prefetch_count=1)
        
        if self._broker_type == self._weather:
            self._run_weather_broker()
        elif self._broker_type == self._stations:
            self._run_stations_broker()
        elif self._broker_type == self._trips:
            self._run_trips_broker()
        else:
            logging.error(f'action: run | result: error | broker_type: {self._broker_type} | broker_number: {self._broker_number} | error: Invalid broker type')
            raise Exception("Invalid broker type")
        
        self._channel.start_consuming()

    def _run_weather_broker(self):
        logging.info(f'action: run_weather_broker | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        self._channel.queue_declare(queue="WE1FILTER", durable=True)
        self._channel.basic_consume(queue=self._broker_type, on_message_callback=self._callback_weather)

    def _run_stations_broker(self):
        logging.info(f'action: run_stations_broker | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        self._channel.queue_declare(queue="ej2solver", durable=True)
        self._channel.queue_declare(queue="SE3FILTER", durable=True)
        self._channel.basic_consume(queue=self._broker_type, on_message_callback=self._callback_stations)

    def _run_trips_broker(self):
        logging.info(f'action: run_trips_broker | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        self._channel.queue_declare(queue="ej1solver", durable=True)
        self._channel.queue_declare(queue="TE2FILTER", durable=True)
        self._channel.queue_declare(queue="ej3solver", durable=True)
        self._channel.basic_consume(queue=self._broker_type, on_message_callback=self._callback_trips)

    def _callback_weather(self, ch, method, properties, body):
        body = body.decode("utf-8")
        #logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        weathers = str(body).split('\n')
        for w in weathers:
            weather = Weather(w)
            weather_for_ej1filter = weather.get_weather_for_ej1filter()
            self._send_data_to_queue("we1", weather_for_ej1filter)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _callback_stations(self, ch, method, properties, body):
        body = body.decode("utf-8")
        #logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        stations = str(body).split('\n')
        for s in stations:
            station = Station(s)
            station_for_ej2solver = station.get_weather_for_ej2solver()
            self._send_data_to_queue("ej2solver", station_for_ej2solver)
            station_for_ej3filter = station.get_station_for_ej3filter()
            self._send_data_to_queue("se3", station_for_ej3filter)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        #logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        trips = str(body).split('\n')
        for t in trips:
            trip = Trip(t)
            trip_for_ej1solver = trip.get_trip_for_ej1solver()
            self._send_data_to_queue("ej1solver", trip_for_ej1solver)
            trip_for_ej2filter = trip.get_trip_for_ej2filter()
            self._send_data_to_queue("te2", trip_for_ej2filter)
            trip_for_ej3solver = trip.get_trip_for_ej3solver()
            self._send_data_to_queue("ej3solver", trip_for_ej3solver)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _send_data_to_queue(self, queue, data):
        self._channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=data,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
        ))
