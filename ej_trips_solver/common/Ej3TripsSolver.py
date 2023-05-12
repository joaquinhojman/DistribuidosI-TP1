import json
import logging
import os
from common.Middleware import Middleware
from haversine import haversine

EJ3SOLVER = "ej3solver"
STATIONS = "stations"
TRIPS = "trips"
STATIONSEJ3FILTER = "stationsej3"
STATIONS_EJ3_EXCHANGE = "stations_ej3_exchange"
EOF = "eof"

class Ej3TripsSolver:
    def __init__(self, ejtripssolver, middleware):
        self._EjTripsSolver = ejtripssolver
        self._id = os.getenv('EJ3TRIPSSOLVER_ID', "")
        self._stations_eof_to_expect = int(os.getenv('SE3FCANT', ""))

        self._middleware: Middleware = middleware
        self._stations_name = {}
        self._montreal_stations = {}
        self._stations_queue = None

        self._initialize_rabbitmq()

    def _initialize_rabbitmq(self):
        self._middleware.exchange_declare(exchange=STATIONS_EJ3_EXCHANGE, exchange_type='fanout')

        self._stations_queue = f'{STATIONSEJ3FILTER}_{self._id}'
        self._middleware.queue_declare(queue=self._stations_queue, durable=True)
        self._middleware.queue_bind(exchange=STATIONS_EJ3_EXCHANGE, queue=self._stations_queue)
        
        self._middleware.queue_declare(queue=EJ3SOLVER, durable=True)

    def run(self):
        logging.info(f'action: run | result: in_progress | EjTripsSolver: {self._EjTripsSolver}')
        self._middleware.basic_qos(prefetch_count=1)
        self._middleware.recv_message(queue=self._stations_queue, callback=self._callback_stations)
        self._middleware.start_consuming()
        logging.info(f'action: run | result: stations getted | EjTripsSolver: {self._EjTripsSolver}')
        self._middleware.basic_qos(prefetch_count=1)
        self._middleware.recv_message(queue=self._EjTripsSolver, callback=self._callback_trips)
        self._middleware.start_consuming()

    def _callback_stations(self, ch, method, properties, body):
        finished = False
        body = body.decode("utf-8")
        data = json.loads(body)
        if data["type"] == STATIONS:
            self._stations_name[str((data["code"], data["yearid"]))] = data["name"]
            self._montreal_stations[data["name"]] = MontrealStation(data["latitude"], data["longitude"])
            self._middleware.send_message(queue=EJ3SOLVER, data=body)
        elif data["type"] == EOF:
            finished = self._process_eof()
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
        self._middleware.send_ack(method.delivery_tag)
        if finished: 
            self._middleware.send_message(queue=EJ3SOLVER, data=body)
            self._middleware.stop_consuming()
    
    def _process_eof(self):
        self._stations_eof_to_expect -= 1
        if self._stations_eof_to_expect == 0:
            return True
        return False

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        trips = body.split("\n")
        for t in trips:
            data = json.loads(t)
            if data["type"] == TRIPS:
                if (str((data["start_station_code"], data["yearid"])) not in self._stations_name) or (str((data["end_station_code"], data["yearid"])) not in self._stations_name):
                    continue

                start_station_name = self._stations_name[str((data["start_station_code"], data["yearid"]))]
                start_sation = self._montreal_stations[start_station_name]
                origin = (start_sation._latitude, start_sation._longitude)

                end_station_name = self._stations_name[str((data["end_station_code"], data["yearid"]))]
                self._montreal_stations[end_station_name].add_trip(origin) 
            elif data["type"] == EOF:
                self._send_trips_to_ej3solver()
                self._middleware.send_ack(method.delivery_tag)
                self._middleware.stop_consuming()
                return
            else:
                logging.error(f'action: _callback_trips | result: error | EjTripsSolver: {self._EjTripsSolver} | error: Invalid type')
        self._middleware.send_ack(method.delivery_tag)
        
    def _send_trips_to_ej3solver(self):
        data = {}
        for k, v in self._montreal_stations.items():
            data[k] = str(v._trips) + "," + str(v._total_km_to_come)
        self._middleware.send_message(queue=EJ3SOLVER, data=str(data))
        logging.info(f'action: _send_trips_to_ej3solver | result: trips sended | EjTripsSolver: {self._EjTripsSolver}')

class MontrealStation:
    def __init__(self, latitude, longitude):
        self._latitude = float(latitude)
        self._longitude = float(longitude)

        self._trips = 0
        self._total_km_to_come = 0.0

    def add_trip(self, origin):
        end = (self._latitude, self._longitude)
        distance_in_km = haversine(origin, end)

        self._trips += 1
        self._total_km_to_come += distance_in_km
