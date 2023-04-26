
import socket
import logging
from time import sleep
import pika

class EofListener:
    def __init__(self):
        self._weather_broker_entities = 0
        self._station_broker_entities = 0
        self._trip_broker_entities = 0

        self._we1_filter_entities = 0
        self._te2_filter_entities = 0
        self._se3_filter_entities = 0

        self._channel = None
        self._create_RabbitMQ_Connection()

    def _create_RabbitMQ_Connection(self):
        logging.info(f'action: create rabbitmq connections | result: in_progress')
        while self._channel is None:
            try: 
                # Create RabbitMQ communication channel
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq'))
                channel = connection.channel()

                channel.queue_declare(queue="eoflistener", durable=True)
                channel.queue_declare(queue="we1", durable=True)
                channel.queue_declare(queue="te2", durable=True)
                channel.queue_declare(queue="se3", durable=True)

                self._channel = channel
            except Exception as e:
                sleep(5)
        logging.info(f'action: create rabbitmq connections | result: success')

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        logging.info(f'action: run | result: in_progress')
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue="eoflistener", on_message_callback=self._callback)
        self._channel.start_consuming()

    def _callback(self, ch, method, properties, body):
        pass
