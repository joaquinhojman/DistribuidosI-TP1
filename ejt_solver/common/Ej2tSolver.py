import json
import logging
import os
import pika

EJ2TRIPS = "ej2trips"
EJ2STATIONS = "ej2stations"

class Ej2tSolver:
    def __init__(self, ejtsolver, channel):
        self._EjtSolver = ejtsolver
        self._channel = channel

        self._stations_name = {}
        self._stations = {}

        channel.queue_declare(queue=EJ2TRIPS, durable=True)
        channel.queue_declare(queue=EJ2STATIONS, durable=True)

    def run(self):
        logging.info(f'action: run | result: in_progress | EjtSolver: {self._EjtSolver}')
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=EJ2STATIONS, on_message_callback=self._callback_stations)
        self._channel.start_consuming()
        logging.info(f'action: run | result: stations getted | EjtSolver: {self._EjtSolver}')
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=self._EjtSolver, on_message_callback=self._callback_trips)
        self._channel.start_consuming()

    def _callback_stations(self, ch, method, properties, body):
        body = body.decode("utf-8")
        data = body.split(";")
        self._stations_name = eval(data[0])
        stations_list = eval(data[1])
        for station in stations_list:
            self._stations[station] = Station()
        ch.basic_ack(delivery_tag=method.delivery_tag)
        self._channel.stop_consuming()

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        data = json.loads(body)
        if data["type"] == "trips": 
            station_name = self._stations_name[str((data["city"], data["start_station_code"], data["yearid"]))]
            self._stations[station_name].add_trip(data["yearid"])
        elif data["type"] == "eof":
            self._send_trips_to_ej2solver()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._channel.stop_consuming()
            return
        else:
            logging.error(f'action: _callback_trips | result: error | EjtSolver: {self._EjtSolver} | error: Invalid type')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def _send_trips_to_ej2solver(self):
        data = {}
        for k, v in self._stations.items():
            data[k] = str(v._trips_on_2016) + "," + str(v._trips_on_2017)
        self._channel.basic_publish(exchange='', routing_key=EJ2TRIPS, body=str(data))
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
