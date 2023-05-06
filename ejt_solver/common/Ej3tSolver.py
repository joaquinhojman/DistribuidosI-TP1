import json
import logging
from common.Middleware import Middleware
from haversine import haversine

EJ3TRIPS = "ej3trips"
EJ3STATIONS = "ej3stations"

class Ej3tSolver:
    def __init__(self, ejtsolver, middleware):
        self._EjtSolver = ejtsolver
        self._middleware: Middleware = middleware

        self._stations_name = {}
        self._montreal_stations = {}

        self._middleware.queue_declare(queue=EJ3TRIPS, durable=True)
        self._middleware.queue_declare(queue=EJ3STATIONS, durable=True)

    def run(self):
        logging.info(f'action: run | result: in_progress | EjtSolver: {self._EjtSolver}')
        self._middleware.basic_qos(prefetch_count=1)
        self._middleware.recv_message(queue=EJ3STATIONS, callback=self._callback_stations)
        self._middleware.start_consuming()
        logging.info(f'action: run | result: stations getted | EjtSolver: {self._EjtSolver}')
        self._middleware.basic_qos(prefetch_count=1)
        self._middleware.recv_message(queue=self._EjtSolver, callback=self._callback_trips)
        self._middleware.start_consuming()

    def _callback_stations(self, ch, method, properties, body):
        body = body.decode("utf-8")
        data = body.split(";")
        self._stations_name = eval(data[0])
        stations_list = eval(data[1])
        for station in stations_list:
            station_data = station.split("+")
            self._montreal_stations[station_data[0]] = MontrealStation(station_data[1], station_data[2])
        ch.basic_ack(delivery_tag=method.delivery_tag)
        self._middleware.stop_consuming()

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        data = json.loads(body)
        if data["type"] == "trips": 
            start_station_name = self._stations_name[str((data["start_station_code"], data["yearid"]))]
            start_sation = self._montreal_stations[start_station_name]
            origin = (start_sation._latitude, start_sation._longitude)

            end_station_name = self._stations_name[str((data["end_station_code"], data["yearid"]))]
            self._montreal_stations[end_station_name].add_trip(origin) 
        elif data["type"] == "eof":
            self._send_trips_to_ej3solver()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._middleware.stop_consuming()
            return
        else:
            logging.error(f'action: _callback_trips | result: error | EjtSolver: {self._EjtSolver} | error: Invalid type')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def _send_trips_to_ej3solver(self):
        data = {}
        for k, v in self._montreal_stations.items():
            data[k] = str(v._trips) + "," + str(v._total_km_to_come)
        self._middleware.send_message(queue=EJ3TRIPS, data=str(data))
        logging.info(f'action: _send_trips_to_ej3solver | result: trips sended | EjtSolver: {self._EjtSolver}')

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
