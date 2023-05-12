import logging
import os
from time import sleep

from common.Middleware import Middleware
from common.types import EOF, StationsEj3, TripsEj2, TripsEj3, WeatherEj1, StationsEj2

EJ1SOLVER = "ej1solver"
EJ2SOLVER = "ej2solver"
EJ3SOLVER = "ej3solver"
EOFTRIPSLISTENER = "eoftripslistener"
EJ1TRIPSSOLVER = "ej1tripssolver"
EJ2TRIPSSOLVER = "ej2tripssolver"
EJ3TRIPSSOLVER = "ej3tripssolver"
WEATHER_EJ1_EXCHANGE = "weather_ej1_exchange"
STATIONS_EJ2_EXCHANGE = "stations_ej2_exchange"
STATIONS_EJ3_EXCHANGE = "stations_ej3_exchange"
_EOF = "eof"

class Filter:
    def __init__(self, filter_type, filter_number, weather_ej1, stations_ej2, trips_ej2, stations_ej3, trips_ej3):
        self._sigterm = False
        self._filter_type = filter_type
        self._filter_number = filter_number
        self._weather_ej1 = weather_ej1
        self._stations_ej2 = stations_ej2
        self._trips_ej2 = trips_ej2
        self._stations_ej3 = stations_ej3
        self._trips_ej3 = trips_ej3

        self._cant_ej_trips_solver = int(os.getenv('EJTRIPSCANT', "0"))
        self._middleware: Middleware = None

    def _sigterm_handler(self, _signo, _stack_frame):
        self._sigterm = True
        if self._middleware is not None:
            self._middleware.close()
        exit(0)

    def _initialize_rabbitmq(self):
        logging.info(f'action: initialize_rabbitmq | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware = Middleware()

        self._middleware.queue_declare(queue=self._filter_type, durable=True)
        self._middleware.queue_declare(queue=EOFTRIPSLISTENER, durable=True)
        logging.info(f'action: initialize_rabbitmq | result: success | filter_type: {self._filter_type} | filter_number: {self._filter_number}')

    def run(self):
        try:
            self._initialize_rabbitmq()
            logging.info(f'action: run | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
            self._middleware.basic_qos(prefetch_count=1)
            
            if self._filter_type == self._weather_ej1:
                self._run_weather_ej1_filter()
            elif self._filter_type == self._stations_ej2:
                self._run_stations_ej2_filter()
            elif self._filter_type == self._trips_ej2:
                self._run_trips_ej2_filter()
            elif self._filter_type == self._stations_ej3:
                self._run_stations_ej3_filter()
            elif self._filter_type == self._trips_ej3:
                self._run_trips_ej3_filter()
            else:
                logging.error(f'action: run | result: error | filter_type: {self._filter_type} | filter_number: {self._filter_number} | error: Invalid filter type')
                raise Exception("Invalid filter type")
            
            self._middleware.start_consuming()
        except Exception as e:
            logging.error(f'action: run | result: error | filter_type: {self._filter_type} | filter_number: {self._filter_number} | error: {e}')
            if self._middleware is not None:
                self._middleware.close()
            exit(0)

    def _run_weather_ej1_filter(self):
        logging.info(f'action: _run_weather_ej1_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.exchange_declare(exchange=WEATHER_EJ1_EXCHANGE, exchange_type='fanout')
        self._create_queues_for_exchange(exchange=WEATHER_EJ1_EXCHANGE, type=self._filter_type, ejtripstsolvercant=self._cant_ej_trips_solver)
        self._middleware.recv_message(queue=self._filter_type, callback=self._callback_weather_ej1)

    def _run_stations_ej2_filter(self):
        logging.info(f'action: _run_stations_ej2_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.exchange_declare(exchange=STATIONS_EJ2_EXCHANGE, exchange_type='fanout')
        self._create_queues_for_exchange(exchange=STATIONS_EJ2_EXCHANGE, type=self._filter_type, ejtripstsolvercant=self._cant_ej_trips_solver)
        self._middleware.recv_message(queue=self._filter_type, callback=self._callback_stations_ej2)

    def _run_trips_ej2_filter(self):
        logging.info(f'action: _run_trips_ej2_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.queue_declare(queue=EJ2TRIPSSOLVER, durable=True)
        self._middleware.recv_message(queue=self._filter_type, callback=self._callback_trips_ej2)

    def _run_stations_ej3_filter(self):
        logging.info(f'action: _run_stations_ej3_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.exchange_declare(exchange=STATIONS_EJ3_EXCHANGE, exchange_type='fanout')
        self._create_queues_for_exchange(exchange=STATIONS_EJ3_EXCHANGE, type=self._filter_type, ejtripstsolvercant=self._cant_ej_trips_solver)
        self._middleware.recv_message(queue=self._filter_type, callback=self._callback_stations_ej3)

    def _run_trips_ej3_filter(self):
        logging.info(f'action: _run_trips_ej3_filter | result: in_progress | filter_type: {self._filter_type} | filter_number: {self._filter_number}')
        self._middleware.queue_declare(queue=EJ3TRIPSSOLVER, durable=True)
        self._middleware.recv_message(queue=self._filter_type, callback=self._callback_trips_ej3)

    def _create_queues_for_exchange(self, exchange, type, ejtripstsolvercant):
        for i in range(1, ejtripstsolvercant + 1):
            queue_name = f'{type}_{i}'
            self._middleware.queue_declare(queue=queue_name, durable=True)
            self._middleware.queue_bind(exchange=exchange, queue=queue_name)

    def _callback_weather_ej1(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body, WEATHER_EJ1_EXCHANGE, method)
        if eof: return
        we1 = WeatherEj1(body)
        if we1.is_valid():
            self._middleware.send_to_exchange(exchange=WEATHER_EJ1_EXCHANGE, message=we1.get_json())
        self._middleware.send_ack(method.delivery_tag)

    def _callback_stations_ej2(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body, STATIONS_EJ2_EXCHANGE, method)
        if eof: return
        se2 = StationsEj2(body)
        if se2.is_valid():
            self._middleware.send_to_exchange(exchange=STATIONS_EJ2_EXCHANGE, message=se2.get_json())
        self._middleware.send_ack(method.delivery_tag)

    def _callback_trips_ej2(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body, EOFTRIPSLISTENER, method)
        if eof: return
        trips = body.split('\n')
        trips_for_ej2tripssolver = []
        for t in trips:
            te2 = TripsEj2(t)
            if te2.is_valid():
                trips_for_ej2tripssolver.append(te2.get_json())
        message = "\n".join(trips_for_ej2tripssolver)
        if message != "":
            self._send_data_to_queue(EJ2TRIPSSOLVER, message)
        self._middleware.send_ack(method.delivery_tag)

    def _callback_stations_ej3(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body, STATIONS_EJ3_EXCHANGE, method)
        if eof: return
        se3 = StationsEj3(body)
        if se3.is_valid():
            self._middleware.send_to_exchange(exchange=STATIONS_EJ3_EXCHANGE, message=se3.get_json())
        self._middleware.send_ack(method.delivery_tag)

    def _callback_trips_ej3(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body, EOFTRIPSLISTENER, method)
        if eof: return
        trips = body.split('\n')
        trips_for_ej3tripssolver = []
        for t in trips:
            te3 = TripsEj3(t)
            if te3.is_valid():
                trips_for_ej3tripssolver.append(te3.get_json())
        message = "\n".join(trips_for_ej3tripssolver)
        if message != "": 
            self._send_data_to_queue(EJ3TRIPSSOLVER, message)
        self._middleware.send_ack(method.delivery_tag)

    def _check_eof(self, body, queue, method):
        if (body[:3] == _EOF):
            if queue == EOFTRIPSLISTENER:
                self._send_eof_to_eoftripslistener()
            else:
                self._send_eof_to_ejtripstsolver(body, queue)
            self._middleware.send_ack(method.delivery_tag)
            self._exit()
            return True
        return False

    def _send_eof_to_ejtripstsolver(self, body, exchange):
        eof = EOF(body.split(",")[1])
        self._middleware.send_to_exchange(exchange=exchange, message=eof.get_json())
        logging.info(f'action: _check_eof | result: success | filter_type: {self._filter_type} | filter_number: {self._filter_number}')

    def _send_eof_to_eoftripslistener(self):
        eof = self._filter_type
        self._send_data_to_queue(EOFTRIPSLISTENER, eof)
        logging.info(f'action: _check_eof | result: success | filter_type: {self._filter_type} | filter_number: {self._filter_number}')

    def _send_data_to_queue(self, queue, data):
        self._middleware.send_message(queue=queue, data=data)

    def _exit(self):
        self._middleware.stop_consuming()
        self._middleware.close()
