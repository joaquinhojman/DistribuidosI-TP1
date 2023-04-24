import logging
import os
import pika

class Broker:
    def __init__(self, broker_type, broker_number, weather, stations, trips):
        self._broker_type = broker_type
        self._broker_number = broker_number
        self._weather = weather
        self._stations = stations
        self._trips = trips

        self._channel = self._initialize_rabbitmq()

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        logging.info(f'action: Handle SIGTERM | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number}')

    def _initialize_rabbitmq(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()

        channel.queue_declare(queue=self._broker_type, durable=True)
        return channel

    def run(self):
        logging.info(f'action: run | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        self._channel.basic_qos(prefetch_count=1)
        
        if self._broker_type is self._weather:
            self._run_weather_broker()
        elif self._broker_type is self._stations:
            self._run_stations_broker()
        elif self._broker_type is self._trips:
            self._run_trips_broker()
        else:
            logging.error(f'action: run | result: error | broker_type: {self._broker_type} | broker_number: {self._broker_number} | error: Invalid broker type')
            raise Exception("Invalid broker type")
        
        self._channel.start_consuming()

    def _run_weather_broker(self):
        self._channel.basic_consume(queue=self._broker_type, on_message_callback=self._callback_weather)

    def _run_stations_broker(self):
        self._channel.basic_consume(queue=self._broker_type, on_message_callback=self._callback_stations)

    def _run_trips_broker(self):
        self._channel.basic_consume(queue=self._broker_type, on_message_callback=self._callback_trips)

    def _callback_weather(self, ch, method, properties, body):
        logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _callback_stations(self, ch, method, properties, body):
        logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _callback_trips(self, ch, method, properties, body):
        logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        ch.basic_ack(delivery_tag=method.delivery_tag)
