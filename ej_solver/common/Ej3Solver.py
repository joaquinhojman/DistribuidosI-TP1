import json
import logging
import os
from haversine import haversine

class Ej3Solver:
    def __init__(self, EjSolver, channel):
        self._EjSolver = EjSolver
        self._channel = channel
        self._montreal_stations = {}

    def run(self):
        logging.info(f'action: run_Ej3Solver | result: in_progress')
        self._channel.basic_consume(queue=self._EjSolver, on_message_callback=self._callback)

    def _callback(self, ch, method, properties, body):
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == "station":
            self._montreal_stations[data["code"]] = MontrealStation(data["name"], data["latitude"], data["longitude"])
        elif data["type"] == "trip":
            latitude_origin = self._montreal_stations[data["start_station_code"]]._latitude
            longitude_origin = self._montreal_stations[data["start_station_code"]]._longitude
            origin = (latitude_origin, longitude_origin)

            self._montreal_stations[data["end_station_code"]].add_trip(origin) 
            pass
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
    
    def _get_results(self):
        results = {}
        for _key, value in self._montreal_stations.items():
            avg_km = value.get_average_km()
            if avg_km > 6:
                results[value._name] = avg_km
        return str(results)

class MontrealStation:
    def __init__(self, name, latitude, longitude):
        self._name = name
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
