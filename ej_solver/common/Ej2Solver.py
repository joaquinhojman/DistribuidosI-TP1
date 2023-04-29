import json
import logging
import os
import pika

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
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == "station":
            self._stations_name[(data["city"], data["code"], data["yearid"])] = data["name"]
            self._stations[data["name"]] = Station()
        elif data["type"] == "trip":
            station_name = self._stations_name[(data["city"], data["start_station_code"], data["yearid"])]
            self._stations[station_name].add_trip(data["yearid"])
        elif data["type"] == "eof":
            self._process_eof(data["eof"])
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
    
    def _process_eof(self, eof):
        if eof == "station":
            self._stations_eof_to_expect -= 1
            if self._stations_eof_to_expect == 0:
                self._send_eof_confirm()
        elif eof == "trip":
            self._trips_eof_to_expect -= 1
            if self._trips_eof_to_expect == 0:
                self._send_results()
                self._exit()
        else:
            logging.error(f'action: _callback | result: error | error: Invalid eof | eof: {eof}')

    def _send_eof_confirm(self, eof):
        json_eof = json.dumps({
            "EjSolver": self._EjSolver,
            "eof": "station"
        })
        self._send(json_eof)

    def _send_results(self):
        results = self._get_results()
        json_results = json.dumps({
            "EjSolver": self._EjSolver,
            "eof": "trip",
            "results": str(results)
        })
        self._send(json_results)

    def _get_results(self):
        results = {[]}
        for key, value in self._stations.items():
            if value.duplicate_trips():
                results[key] = (value._trips_on_2016, value._trips_on_2017)
        return results
    
    def _send(self, data):
        self._channel.basic_publish(
            exchange='',
            routing_key='results',
            body=data,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
        ))
        logging.info(f'action: _send | result: success | data: {data}')

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
        return self.add_2016_trip * 2 <= self.add_2017_trip
