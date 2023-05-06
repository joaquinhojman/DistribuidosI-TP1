import logging
import os
from time import sleep

from common.Middleware import Middleware
from common.types import EOF, Se3, Te2, Te3, We1, Se2

EJ1SOLVER = "ej1solver"
EJ2SOLVER = "ej2solver"
EJ3SOLVER = "ej3solver"
EOFTLISTENER = "eoftlistener"
EJ2TSOLVER = "ej2tsolver"
EJ3TSOLVER = "ej3tsolver"

class Filter:
    def __init__(self, filter_type, filter_number, we1, se2, te2, se3, te3):
        self._sigterm = False
        self._filter_type = filter_type
        self._filter_number = filter_number
        self._we1 = we1
        self._se2 = se2
        self._te2 = te2
        self._se3 = se3
        self._te3 = te3

        self._middleware: Middleware = None

    def _sigterm_handler(self, _signo, _stack_frame):
        self._sigterm = True
        if self._middleware is not None:
            self._middleware.close()
        exit(0)

    def _initialize_rabbitmq(self):
        logging.info(f'action: initialize_rabbitmq | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        retries =  int(os.getenv('RMQRETRIES', "5"))
        while retries > 0 and self._middleware is None:
            sleep(15)
            retries -= 1
            try:
                self._middleware = Middleware()

                self._middleware.queue_declare(queue=self._filter_type, durable=True)
                self._middleware.queue_declare(queue=EOFTLISTENER, durable=True)
            except Exception as e:
                if self._sigterm: exit(0)
                pass
        logging.info(f'action: initialize_rabbitmq | result: success | filter_type: {self._filter_type} | filter_number: {self._filter_number}')

    def run(self):
        try:
            self._initialize_rabbitmq()
            logging.info(f'action: run | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
            self._middleware.basic_qos(prefetch_count=1)
            
            if self._filter_type == self._we1:
                self._run_we1_filter()
            elif self._filter_type == self._se2:
                self._run_se2_filter()
            elif self._filter_type == self._te2:
                self._run_te2_filter()
            elif self._filter_type == self._se3:
                self._run_se3_filter()
            elif self._filter_type == self._te3:
                self._run_te3_filter()
            else:
                logging.error(f'action: run | result: error | filter_type: {self._filter_type} | filter_number: {self._filter_number} | error: Invalid filter type')
                raise Exception("Invalid filter type")
            
            self._middleware.start_consuming()
        except Exception as e:
            logging.error(f'action: run | result: error | filter_type: {self._filter_type} | filter_number: {self._filter_number} | error: {e}')
            if self._middleware is not None:
                self._middleware.close()
            exit(0)

    def _run_we1_filter(self):
        logging.info(f'action: _run_we1_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.queue_declare(queue=EJ1SOLVER, durable=True)
        self._middleware.recv_message(queue=self._filter_type, callback=self._callback_we1)

    def _run_se2_filter(self):
        logging.info(f'action: _run_se2_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.queue_declare(queue=EJ2TSOLVER, durable=True)
        self._middleware.recv_message(queue=self._filter_type, callback=self._callback_se2)

    def _run_te2_filter(self):
        logging.info(f'action: _run_te2_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.queue_declare(queue=EJ2TSOLVER, durable=True)
        self._middleware.recv_message(queue=self._filter_type, callback=self._callback_te2)

    def _run_se3_filter(self):
        logging.info(f'action: _run_se3_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.queue_declare(queue=EJ3SOLVER, durable=True)
        self._middleware.recv_message(queue=self._filter_type, callback=self._callback_se3)

    def _run_te3_filter(self):
        logging.info(f'action: _run_te3_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.queue_declare(queue=EJ3TSOLVER, durable=True)
        self._middleware.recv_message(queue=self._filter_type, callback=self._callback_te3)


    def _callback_we1(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body, EJ1SOLVER, ch, method)
        if eof: return
        we1 = We1(body)
        if we1.is_valid():
            self._send_data_to_queue(EJ1SOLVER, we1.get_json())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _callback_se2(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body, EJ2SOLVER, ch, method)
        if eof: return
        se2 = Se2(body)
        if se2.is_valid():
            self._send_data_to_queue(EJ2SOLVER, se2.get_json())
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
        eof = self._filter_type
        self._send_data_to_queue(EOFTLISTENER, eof)
        logging.info(f'action: _check_eof | result: success | filter_type: {self._filter_type} | filter_number: {self._filter_number}')

    def _send_data_to_queue(self, queue, data):
        self._middleware.send_message(queue=queue, data=data)

    def _exit(self):
        self._middleware.stop_consuming()
        self._middleware.close()
