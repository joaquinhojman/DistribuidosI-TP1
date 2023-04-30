import json
import logging
import os
import pika

STATIONS = "stations"
TRIPS = "trips"
RESULTS = "results"

class Ej2Solver:
    def __init__(self, EjSolver, channel):
        self._EjSolver = EjSolver
        self._channel = channel

        self._stations_eof_to_expect = int(os.getenv('SBRKCANT', ""))
        self._trips_eof_to_expect = int(os.getenv('TE2FCANT', ""))

        self._stations_name = {}
        self._stations = {}

    def run(self):
        logging.info(f'action: run_Ej2Solver | result: in_progress')
        self._channel.basic_consume(queue=self._EjSolver, on_message_callback=self._callback)

    def _callback(self, ch, method, properties, body):
        finished = False
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == STATIONS:
            self._stations_name[(data["city"], data["code"], data["yearid"])] = data["name"]
            self._stations[data["name"]] = Station()
        elif data["type"] == TRIPS:
            station_name = self._stations_name[(data["city"], data["start_station_code"], data["yearid"])]
            self._stations[station_name].add_trip(data["yearid"])
        elif data["type"] == "eof":
            finished = self._process_eof(data["eof"])
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if finished: self._exit()
    
    def _process_eof(self, eof):
        if eof == STATIONS:
            self._stations_eof_to_expect -= 1
            if self._stations_eof_to_expect == 0:
                self._send_eof_confirm()
        elif eof == TRIPS:
            self._trips_eof_to_expect -= 1
            if self._trips_eof_to_expect == 0:
                self._send_results()
                return True
        else:
            logging.error(f'action: _callback | result: error | error: Invalid eof | eof: {eof}')
        return False

    def _send_eof_confirm(self):
        json_eof = json.dumps({
            "EjSolver": self._EjSolver,
            "eof": STATIONS
        })
        self._send(json_eof)

    def _send_results(self):
        results = self._get_results()
        json_results = json.dumps({
            "EjSolver": self._EjSolver,
            "eof": TRIPS,
            "results": str(results)
        })
        self._send(json_results)

    def _get_results(self):
        results = {}
        for key, value in self._stations.items():
            if value.duplicate_trips():
                results[key] = (value._trips_on_2016, value._trips_on_2017)
        return results
    
    def _send(self, data):
        self._channel.basic_publish(
            exchange='',
            routing_key=RESULTS,
            body=data,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
        ))
        logging.info(f'action: _send_results | result: success')

    def _exit(self):
        self._channel.stop_consuming()
        self._channel.close()

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

    def duplicate_trips(self):
        return (self._trips_on_2016 * 2 <= self._trips_on_2017) and self._trips_on_2017 != 0
