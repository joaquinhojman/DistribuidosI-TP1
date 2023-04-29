
import os
import socket
import logging
from time import sleep
import pika

WEATHER = "weather"
STATIONS = "stations"
TRIPS = "trips"
WE1 = "we1"
TE2 = "te2"
SE3 = "se3"
TE3 = "te3"

class EofListener:
    def __init__(self):
        self._remaining_brokers_eof = {
            WEATHER: int(os.getenv('WBRKCANT', "")),
            STATIONS: int(os.getenv('SBRKCANT', "")),
            TRIPS: int(os.getenv('TBRKCANT', ""))
        }

        self._cant_filters = {
            WE1: int(os.getenv('WE1FCANT', "")),
            TE2: int(os.getenv('TE2FCANT', "")),
            SE3: int(os.getenv('SE3FCANT', "")),
            TE3: int(os.getenv('TE3FCANT', ""))
        }

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
                channel.queue_declare(queue=WE1, durable=True)
                channel.queue_declare(queue=TE2, durable=True)
                channel.queue_declare(queue=SE3, durable=True)
                channel.queue_declare(queue=TE3, durable=True)

                self._channel = channel
            except Exception as e:
                sleep(15)
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
        self._proccess_eof(body.decode("utf-8"))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _proccess_eof(self, body):
        self._remaining_brokers_eof[body] -= 1
        if self._remaining_brokers_eof[body] == 0:
            self._send_eofs(body)

    def _send_eofs(self, body):
        if body == WEATHER:
            for _ in range(self._cant_filters[WE1]):
                self._send(WE1, "EOF,"+body)
        elif body == STATIONS:
            for _ in range(self._cant_filters[SE3]):
                self._send(SE3, "EOF,"+body)
        elif body == TRIPS:
            for _ in range(self._cant_filters[TE2]):
                self._send(TE2, "EOF,"+body)
            for _ in range(self._cant_filters[TE3]):
                self._send(TE3, "EOF,"+body)
            self._exit()
        else:
            logging.error(f'action: send eof | result: error | error: invalid body')
            return
        logging.info(f'action: send eof | result: success | body: {body}')

    def _send(self, queue, data):
        self._channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=data,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
        ))

    def _exit(self):
        self._channel.stop_consuming()
        self._channel.close()
