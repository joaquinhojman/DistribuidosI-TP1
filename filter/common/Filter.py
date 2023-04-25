import logging
import os
from time import sleep
import pika

class Filter:
    def __init__(self, filter_type, filter_number, we1, te2, se3):
        self._filter_type = filter_type
        self._filter_number = filter_number
        self._we1 = we1
        self._te2 = te2
        self._se3 = se3

        self._channel = None
        self._initialize_rabbitmq()

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        logging.info(f'action: Handle SIGTERM | result: success | filter_type: {self._filter_type} | filter_number: {self._filter_number}')

    def _initialize_rabbitmq(self):
        logging.info(f'action: initialize_rabbitmq | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        while self._channel is None:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq'))
                channel = connection.channel()

                channel.queue_declare(queue=self._filter_type, durable=True)
                self._channel = channel
            except Exception as e:
                sleep(5)
        logging.info(f'action: initialize_rabbitmq | result: success | filter_type: {self._filter_type} | filter_number: {self._filter_number}')

    def run(self):
        pass
