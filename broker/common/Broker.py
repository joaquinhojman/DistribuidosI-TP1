import logging
import os
from time import sleep

from common.Middleware import Middleware
from common.types import Station, Trip, Weather

EOFLISTENER = "eoflistener"
EOFTLISTENER = "eoftlistener"
EJ1TSOLVER = "ej1tsolver"
EJ2SOLVER = "ej2solver"
EJ3SOLVER = "ej3solver"
WE1 = "we1"
SE2 = "se2"
TE2 = "te2"
SE3 = "se3"
TE3 = "te3"
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
        retries =  int(os.getenv('RMQRETRIES', "5"))
        while retries > 0 and self._middleware is None:
            sleep(15)
            retries -= 1
            try:
                self._middleware = Middleware()

                self._middleware.queue_declare(queue=self._broker_type, durable=True)
                self._middleware.queue_declare(queue=EOFLISTENER, durable=True)
                self._middleware.queue_declare(queue=EOFTLISTENER, durable=True)

            except Exception as e:
                if self._sigterm: exit(0)
                pass
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
        self._middleware.queue_declare(queue=WE1, durable=True)
        self._middleware.recv_message(queue=self._broker_type, callback=self._callback_weather)

    def _run_stations_broker(self):
        logging.info(f'action: run_stations_broker | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        self._middleware.queue_declare(queue=SE2, durable=True)
        self._middleware.queue_declare(queue=SE3, durable=True)
        self._middleware.recv_message(queue=self._broker_type, callback=self._callback_stations)

    def _run_trips_broker(self):
        logging.info(f'action: run_trips_broker | result: in_progress | broker_type: {self._broker_type} | broker_number: {self._broker_number}')
        self._middleware.queue_declare(queue=EJ1TSOLVER, durable=True)
        self._middleware.queue_declare(queue=TE2, durable=True)
        self._middleware.queue_declare(queue=TE3, durable=True)
        self._middleware.recv_message(queue=self._broker_type, callback=self._callback_trips)

    def _callback_weather(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body[:3], method)
        if eof: return
        #logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        weathers = body.split('\n')
        for w in weathers:
            weather = Weather(w)
            weather_for_ej1filter = weather.get_weather_for_ej1filter()
            self._send_data_to_queue(WE1, weather_for_ej1filter)
        self._middleware.send_ack(method.delivery_tag)

    def _callback_stations(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body[:3], method)
        if eof: return
        #logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        stations = body.split('\n')
        for s in stations:
            station = Station(s)
            station_for_ej2solver = station.get_station_for_ej2filter()
            self._send_data_to_queue(SE2, station_for_ej2solver)
            station_for_ej3filter = station.get_station_for_ej3filter()
            self._send_data_to_queue(SE3, station_for_ej3filter)
        self._middleware.send_ack(method.delivery_tag)

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        eof = self._check_eof(body[:3], method)
        if eof: return
        #logging.info(f'action: callback | result: success | broker_type: {self._broker_type} | broker_number: {self._broker_number} | body: {body}')
        trips = body.split('\n')
        for t in trips:
            trip = Trip(t)
            trip_for_ej1solver = trip.get_trip_for_ej1solver()
            self._send_data_to_queue(EJ1TSOLVER, trip_for_ej1solver)
            trip_for_ej2filter = trip.get_trip_for_ej2filter()
            self._send_data_to_queue(TE2, trip_for_ej2filter)
            trip_for_ej3solver = trip.get_trip_for_ej3filter()
            self._send_data_to_queue(TE3, trip_for_ej3solver)
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
            self._send_data_to_queue(EOFTLISTENER, TRIPS)
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
