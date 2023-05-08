import json
import logging
import os
from common.Middleware import Middleware

EJ2SOLVER = "ej2solver"
STATIONS = "stations"
SE2FILTER = "se2"
STATIONS_EJ2_EXCHANGE = "stations_ej2_exchange"

class Ej2tSolver:
    def __init__(self, ejtsolver, middleware):
        self._EjtSolver = ejtsolver
        self._id = os.getenv('EJ2TSOLVER_ID', "")
        self._stations_eof_to_expect = int(os.getenv('SE2FCANT', ""))

        self._middleware: Middleware = middleware
        self._stations_name = {}
        self._stations = {}
        self._stations_queue = None

        self._initialize_rabbitmq()

    def _initialize_rabbitmq(self):
        self._middleware.exchange_declare(exchange=STATIONS_EJ2_EXCHANGE, exchange_type='fanout')

        self._stations_queue = f'{SE2FILTER}_{self._id}'
        self._middleware.queue_declare(queue=self._stations_queue, durable=True)
        self._middleware.queue_bind(exchange=STATIONS_EJ2_EXCHANGE, queue=self._stations_queue)
        
        self._middleware.queue_declare(queue=EJ2SOLVER, durable=True)

    def run(self):
        logging.info(f'action: run | result: in_progress | EjtSolver: {self._EjtSolver}')
        self._middleware.basic_qos(prefetch_count=1)
        self._middleware.recv_message(queue=self._stations_queue, callback=self._callback_stations)
        self._middleware.start_consuming()
        logging.info(f'action: run | result: stations getted | EjtSolver: {self._EjtSolver}')
        self._middleware.basic_qos(prefetch_count=1)
        self._middleware.recv_message(queue=self._EjtSolver, callback=self._callback_trips)
        self._middleware.start_consuming()

    def _callback_stations(self, ch, method, properties, body):
        finished = False
        body = body.decode("utf-8")
        data = json.loads(body)
        if data["type"] == STATIONS:
            self._stations_name[str((data["city"], data["code"], data["yearid"]))] = data["name"]
            self._stations[data["name"]] = Station()
            self._middleware.send_message(queue=EJ2SOLVER, data=body)
        elif data["type"] == "eof":
            finished = self._process_eof()
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
        self._middleware.send_ack(method.delivery_tag)
        if finished:
            self._middleware.send_message(queue=EJ2SOLVER, data=body)
            self._middleware.stop_consuming()
    
    def _process_eof(self):
        self._stations_eof_to_expect -= 1
        if self._stations_eof_to_expect == 0:
            return True
        return False

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        data = json.loads(body)
        if data["type"] == "trips": 
            station_name = self._stations_name[str((data["city"], data["start_station_code"], data["yearid"]))]
            self._stations[station_name].add_trip(data["yearid"])
        elif data["type"] == "eof":
            self._send_trips_to_ej2solver()
            self._middleware.send_ack(method.delivery_tag)
            self._middleware.stop_consuming()
            return
        else:
            logging.error(f'action: _callback_trips | result: error | EjtSolver: {self._EjtSolver} | error: Invalid type')
        self._middleware.send_ack(method.delivery_tag)
        
    def _send_trips_to_ej2solver(self):
        data = {}
        for k, v in self._stations.items():
            data[k] = str(v._trips_on_2016) + "," + str(v._trips_on_2017)
        self._middleware.send_message(queue=EJ2SOLVER, data=str(data))
        logging.info(f'action: _send_trips_to_ej2solver | result: trips sended | EjtSolver: {self._EjtSolver}')

class Station:
    def __init__(self):
        self._trips_on_2016 = 0
        self._trips_on_2017 = 0
    
    def add_trip(self, year):
        if year == "2016":
            self._trips_on_2016 += 1
        elif year == "2017":
            self._trips_on_2017 += 1
        else:
            logging.error(f'action: add_trip | result: error | error: Invalid year | year: {year}')
