import json
import logging
import os
from haversine import haversine
import pika

class Ej3Solver:
    def __init__(self, EjSolver, channel):
        self._EjSolver = EjSolver
        self._channel = channel
        
        self._stations_eof_to_expect = int(os.getenv('SE3FCANT', ""))
        self._trips_eof_to_expect = int(os.getenv('TE3FCANT', ""))

        self._stations_name = {}
        self._montreal_stations = {}

    def run(self):
        logging.info(f'action: run_Ej3Solver | result: in_progress')
        self._channel.basic_consume(queue=self._EjSolver, on_message_callback=self._callback)

    def _callback(self, ch, method, properties, body):
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == "station":
            self._stations_name[(data["code"], data["yearid"])] = data["name"]
            self._montreal_stations[data["name"]] = MontrealStation(data["latitude"], data["longitude"])
        elif data["type"] == "trip":
            start_station_name = self._stations_name[(data["start_station_code"], data["yearid"])]
            start_sation = self._montreal_stations[start_station_name]
            origin = (start_sation._latitude, start_sation._longitude)

            end_station_name = self._stations_name[(data["end_station_code"], data["yearid"])]
            self._montreal_stations[end_station_name].add_trip(origin) 
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
        results = {}
        for key, value in self._montreal_stations.items():
            avg_km = value.get_average_km()
            if avg_km > 6:
                results[key] = avg_km
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

class MontrealStation:
    def __init__(self, latitude, longitude):
        self._latitude = float(latitude)
        self._longitude = float(longitude)

        self._trips = 0
        self._total_km_to_come = 0

    def add_trip(self, origin):
        end = (self._latitude, self._longitude)
        distance_in_km = haversine(origin, end)

        self._trips += 1
        self._total_km_to_come += distance_in_km
    
    def get_average_km(self):
        return self._total_km_to_come / self._trips
