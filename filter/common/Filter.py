import logging
import os
from time import sleep
import pika

from common.types import EOF, Se3, Te2, Te3, We1

EJ1SOLVER = "ej1solver"
EJ2SOLVER = "ej2solver"
EJ3SOLVER = "ej3solver"
EOFTLISTENER = "eoftlistener"
EJ2TSOLVER = "ej2tsolver"
EJ3TSOLVER = "ej3tsolver"

class Filter:
    def __init__(self, filter_type, filter_number, we1, te2, se3, te3):
        self._filter_type = filter_type
        self._filter_number = filter_number
        self._we1 = we1
        self._te2 = te2
        self._se3 = se3
        self._te3 = te3

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
                channel.queue_declare(queue=EOFTLISTENER, durable=True)
                self._channel = channel
            except Exception as e:
                sleep(15)
        logging.info(f'action: initialize_rabbitmq | result: success | filter_type: {self._filter_type} | filter_number: {self._filter_number}')

    def run(self):
        logging.info(f'action: run | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._channel.basic_qos(prefetch_count=1)
        
        if self._filter_type == self._we1:
            self._run_we1_filter()
        elif self._filter_type == self._te2:
            self._run_te2_filter()
        elif self._filter_type == self._se3:
            self._run_se3_filter()
        elif self._filter_type == self._te3:
            self._run_te3_filter()
        else:
            logging.error(f'action: run | result: error | filter_type: {self._filter_type} | filter_number: {self._filter_number} | error: Invalid filter type')
            raise Exception("Invalid filter type")
        
        self._channel.start_consuming()

    def _run_we1_filter(self):
        logging.info(f'action: _run_we1_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._channel.queue_declare(queue=EJ1SOLVER, durable=True)
        self._channel.basic_consume(queue=self._filter_type, on_message_callback=self._callback_we1)

    def _run_te2_filter(self):
        logging.info(f'action: _run_te2_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._channel.queue_declare(queue=EJ2TSOLVER, durable=True)
        self._channel.basic_consume(queue=self._filter_type, on_message_callback=self._callback_te2)

    def _run_se3_filter(self):
        logging.info(f'action: _run_se3_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._channel.queue_declare(queue=EJ3SOLVER, durable=True)
        self._channel.basic_consume(queue=self._filter_type, on_message_callback=self._callback_se3)

    def _run_te3_filter(self):
        logging.info(f'action: _run_te3_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._channel.queue_declare(queue=EJ3TSOLVER, durable=True)
        self._channel.basic_consume(queue=self._filter_type, on_message_callback=self._callback_te3)


    def _callback_we1(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body, EJ1SOLVER, ch, method)
        if eof: return
        we1 = We1(body)
        if we1.is_valid():
            self._send_data_to_queue(EJ1SOLVER, we1.get_json())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _callback_te2(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body, EOFTLISTENER, ch, method)
        if eof: return
        te2 = Te2(body)
        if te2.is_valid():
            self._send_data_to_queue(EJ2TSOLVER, te2.get_json())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _callback_se3(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body, EJ3SOLVER, ch, method)
        if eof: return
        se3 = Se3(body)
        if se3.is_valid():
            self._send_data_to_queue(EJ3SOLVER, se3.get_json())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _callback_te3(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body, EOFTLISTENER, ch, method)
        if eof: return
        te3 = Te3(body)
        if te3.is_valid():
            self._send_data_to_queue(EJ3TSOLVER, te3.get_json())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _check_eof(self, body, queue, ch, method):
        if (body[:3] == "EOF"):
            if queue == EOFTLISTENER:
                self._send_eof_to_eoftlistener()
            else:
                self._send_eof_to_solver(body, queue)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._exit()
            return True
        return False

    def _send_eof_to_solver(self, body, queue):
        eof = EOF(body.split(",")[1])
        self._send_data_to_queue(queue, eof.get_json())
        logging.info(f'action: _check_eof | result: success | filter_type: {self._filter_type} | filter_number: {self._filter_number}')

    def _send_eof_to_eoftlistener(self):
        eof = "eof"
        self._send_data_to_queue(EOFTLISTENER, eof)
        logging.info(f'action: _check_eof | result: success | filter_type: {self._filter_type} | filter_number: {self._filter_number}')

    def _send_data_to_queue(self, queue, data):
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
