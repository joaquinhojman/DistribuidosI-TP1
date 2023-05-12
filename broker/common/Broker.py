import json
import logging
import os
from time import sleep

from common.Middleware import Middleware
from common.types import Station, Trip, Weather

EOFLISTENER = "eoflistener"
EOFTRIPSLISTENER = "eoftripslistener"
EJ1TRIPSSOLVER = "ej1tripssolver"
EJ2SOLVER = "ej2solver"
EJ3SOLVER = "ej3solver"
WEATHEREJ1FILTER = "weatherej1"
STATIONSEJ2FILTER = "stationsej2"
TRIPSEJ2FILTER = "tripsej2"
STATIONSEJ3FILTER = "stationsej3"
TRIPSEJ3FILTER = "tripsej3"
EOF = "eof"
TRIPS = "trips"

class Broker:
    def __init__(self, broker_type, broker_number, weather, stations, trips):
        self._sigterm = False
        self._broker_type = broker_type
        self._broker_number = broker_number
        self._weather = weather
        self._stations = stations
        self._trips = trips

        self._middleware: Middleware = None

    def _sigterm_handler(self, _signo, _stack_frame):
        self._sigterm = True
        if self._middleware is not None:
            self._middleware.close()
        exit(0)

    def _initialize_rabbitmq(self):
        logging.info(f'action: initialize_rabbitmq | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        self._middleware = Middleware()

        self._middleware.queue_declare(queue=self._broker_type, durable=True)
        self._middleware.queue_declare(queue=EOFLISTENER, durable=True)
        self._middleware.queue_declare(queue=EOFTRIPSLISTENER, durable=True)
        logging.info(f'action: initialize_rabbitmq | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number}')

    def run(self):
        try:
            self._initialize_rabbitmq()
            logging.info(f'action: run | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
            self._middleware.basic_qos(prefetch_count=1)

            if self._broker_type == self._weather:
                self._run_weather_broker()
            elif self._broker_type == self._stations:
                self._run_stations_broker()
            elif self._broker_type == self._trips:
                self._run_trips_broker()
            else:
                logging.error(f'action: run | result: error | broker_type: {self._broker_type} | broker_number: {self._broker_number} | error: Invalid broker type')
                raise Exception("Invalid broker type")

            self._middleware.start_consuming()
        except Exception as e:
            logging.error(f'action: run | result: error | broker_type: {self._broker_type} | broker_number: {self._broker_number} | error: {e}')
            if self._middleware is not None:
                self._middleware.close()
            exit(0)

    def _run_weather_broker(self):
        logging.info(f'action: run_weather_broker | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        self._middleware.queue_declare(queue=WEATHEREJ1FILTER, durable=True)
        self._middleware.recv_message(queue=self._broker_type, callback=self._callback_weather)

    def _run_stations_broker(self):
        logging.info(f'action: run_stations_broker | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        self._middleware.queue_declare(queue=STATIONSEJ2FILTER, durable=True)
        self._middleware.queue_declare(queue=STATIONSEJ3FILTER, durable=True)
        self._middleware.recv_message(queue=self._broker_type, callback=self._callback_stations)

    def _run_trips_broker(self):
        logging.info(f'action: run_trips_broker | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        self._middleware.queue_declare(queue=EJ1TRIPSSOLVER, durable=True)
        self._middleware.queue_declare(queue=TRIPSEJ2FILTER, durable=True)
        self._middleware.queue_declare(queue=TRIPSEJ3FILTER, durable=True)
        self._middleware.recv_message(queue=self._broker_type, callback=self._callback_trips)

    def _callback_weather(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body[:3], method)
        if eof: return
        #logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        weathers = body.split('\n')
        for w in weathers:
            try:
                weather = Weather(w)
            except json.decoder.JSONDecodeError as _e:
                continue
            weather_for_ej1filter = weather.get_weather_for_ej1filter()
            self._send_data_to_queue(WEATHEREJ1FILTER, weather_for_ej1filter)
        self._middleware.send_ack(method.delivery_tag)

    def _callback_stations(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body[:3], method)
        if eof: return
        #logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        stations = body.split('\n')
        for s in stations:
            try:
                station = Station(s)
            except json.decoder.JSONDecodeError as _e:
                continue
            station_for_ej2solver = station.get_station_for_ej2filter()
            self._send_data_to_queue(STATIONSEJ2FILTER, station_for_ej2solver)
            station_for_ej3filter = station.get_station_for_ej3filter()
            self._send_data_to_queue(STATIONSEJ3FILTER, station_for_ej3filter)
        self._middleware.send_ack(method.delivery_tag)

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body[:3], method)
        if eof: return
        #logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        trips = body.split('\n')
        trips_for_ej1tripssolver = []
        trips_for_ej2filter = []
        trips_for_ej3solver = []
        for t in trips:
            try:
                trip = Trip(t)
            except json.decoder.JSONDecodeError as _e:
                continue
            trips_for_ej1tripssolver.append(trip.get_trip_for_ej1solver())
            trips_for_ej2filter.append(trip.get_trip_for_ej2filter())
            trips_for_ej3solver.append(trip.get_trip_for_ej3filter())
        self._send_data_to_queue(EJ1TRIPSSOLVER, "\n".join(trips_for_ej1tripssolver))
        self._send_data_to_queue(TRIPSEJ2FILTER, "\n".join(trips_for_ej2filter))
        self._send_data_to_queue(TRIPSEJ3FILTER, "\n".join(trips_for_ej3solver))
        self._middleware.send_ack(method.delivery_tag)

    def _check_eof(self, body, method):
        if body == EOF:
            self._send_eof()
            self._middleware.send_ack(method.delivery_tag)
            self._exit()
            return True
        return False
    
    def _send_eof(self):
        if self._broker_type == self._weather:
            self._send_data_to_queue(EOFLISTENER, self._broker_type)
        elif self._broker_type == self._stations:
            self._send_data_to_queue(EOFLISTENER, self._broker_type)
        elif self._broker_type == self._trips:
            self._send_data_to_queue(EOFTRIPSLISTENER, TRIPS)
            self._send_data_to_queue(EOFLISTENER, self._broker_type)
        else:
            logging.error(f'action: send_eof | result: error | broker_type: {self._broker_type} | broker_number: {self._broker_number} | error: Invalid broker type')
            return
        logging.info(f'action: send_eof | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number}')

    def _send_data_to_queue(self, queue, data):
        self._middleware.send_message(queue=queue, data=data)

    def _exit(self):
        self._middleware.stop_consuming()
        self._middleware.close()
