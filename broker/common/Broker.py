import json
import logging

from common.middleware import BrokerMiddleware
from common.types import Station, Trip, Weather

EOF = "eof"
TRIPS = "trips"

class Broker:
    def __init__(self, broker_type, broker_number, weather, stations, trips, middleware):
        self._sigterm = False
        self._broker_type = broker_type
        self._broker_number = broker_number
        self._weather = weather
        self._stations = stations
        self._trips = trips

        self._middleware: BrokerMiddleware = middleware

    def _sigterm_handler(self, _signo, _stack_frame):
        self._sigterm = True
        if self._middleware is not None:
            self._middleware.close()
        exit(0)

    def run(self):
        try:
            logging.info(f'action: run | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
            if self._broker_type == self._weather:
                self._middleware.recv_weather(self._callback_weather)
            elif self._broker_type == self._stations:
                self._middleware.recv_stations(self._callback_stations)
            elif self._broker_type == self._trips:
                self._middleware.recv_trips(self._callback_trips)
            else:
                logging.error(f'action: run | result: error | broker_type: {self._broker_type} | broker_number: {self._broker_number} | error: Invalid broker type')
                raise Exception("Invalid broker type")
        except Exception as e:
            logging.error(f'action: run | result: error | broker_type: {self._broker_type} | broker_number: {self._broker_number} | error: {e}')
            if self._middleware is not None:
                self._middleware.close()
            exit(0)

    def _callback_weather(self, body, method=None):
        eof = self._check_eof(body[:3], method)
        if eof: return
        weathers = body.split('\n')
        for w in weathers:
            try:
                weather = Weather(w)
            except json.decoder.JSONDecodeError as _e:
                continue
            weather_for_ej1filter = weather.get_weather_for_ej1filter()
            self._middleware.send_weather(weather_for_ej1filter)
        self._middleware.finished_message_processing(method)

    def _callback_stations(self, body, method=None):
        eof = self._check_eof(body[:3], method)
        if eof: return
        stations = body.split('\n')
        for s in stations:
            try:
                station = Station(s)
            except json.decoder.JSONDecodeError as _e:
                continue
            station_for_ej2solver = station.get_station_for_ej2filter()
            station_for_ej3filter = station.get_station_for_ej3filter()
            self._middleware.send_station(station_for_ej2solver, station_for_ej3filter)
        self._middleware.finished_message_processing(method)

    def _callback_trips(self, body, method=None):
        eof = self._check_eof(body[:3], method)
        if eof: return
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

        msg_trips_for_ej1tripssolver = "\n".join(trips_for_ej1tripssolver)
        msg_trips_for_ej2filter = "\n".join(trips_for_ej2filter)
        msg_trips_for_ej3solver = "\n".join(trips_for_ej3solver)
        self._middleware.send_trips(msg_trips_for_ej1tripssolver, msg_trips_for_ej2filter, msg_trips_for_ej3solver)
        self._middleware.finished_message_processing(method)

    def _check_eof(self, body, method):
        if body == EOF:
            self._send_eof()
            self._middleware.finished_message_processing(method)
            self._exit()
            return True
        return False
    
    def _send_eof(self):
        self._middleware.send_eof_to_eof_listener(self._broker_type)
        if self._broker_type == self._trips:
            self._middleware.send_eof_to_eof_trips_listener(TRIPS)
        logging.info(f'action: send_eof | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number}')

    def _exit(self):
        self._middleware.close()
