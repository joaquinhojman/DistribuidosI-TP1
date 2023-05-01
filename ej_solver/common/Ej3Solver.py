import json
import logging
import os
from haversine import haversine
import pika

STATIONS = "stations"
TRIPS = "trips"
EJ3TRIPS = "ej3trips"
EJ3STATIONS = "ej3stations"
RESULTS = "results"

class Ej3Solver:
    def __init__(self, EjSolver, channel):
        self._EjSolver = EjSolver
        self._channel = channel
        
        self._stations_eof_to_expect = int(os.getenv('SE3FCANT', ""))
        self._ej3tsolvers_cant = int(os.getenv('EJ3TCANT', ""))

        self._stations_name = {}
        self._montreal_stations = {}
        channel.queue_declare(queue=EJ3TRIPS, durable=True)
        channel.queue_declare(queue=EJ3STATIONS, durable=True)

    def run(self):
        logging.info(f'action: run_Ej3Solver | result: in_progress')
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=self._EjSolver, on_message_callback=self._callback)
        self._channel.start_consuming()
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=EJ3TRIPS, on_message_callback=self._callback_trips)
        self._channel.start_consuming()

    def _callback(self, ch, method, properties, body):
        finished = False
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == STATIONS:
            self._stations_name[str((data["code"], data["yearid"]))] = data["name"]
            self._montreal_stations[data["name"]] = MontrealStation(data["latitude"], data["longitude"])
        elif data["type"] == "eof":
            finished = self._process_eof()
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if finished: self._channel.stop_consuming()
    
    def _process_eof(self):
        self._stations_eof_to_expect -= 1
        if self._stations_eof_to_expect == 0:
            self._send_stations_to_ejt3solver()
            self._send_eof_confirm()
            return True
        return False
        
    def _send_stations_to_ejt3solver(self):
        stations_info = []
        for key, value in self._montreal_stations.items():
            stations_info.append(str(key)+"+"+str(value._latitude) +"+"+str(value._longitude))
        data = str(self._stations_name) + ";" + str(stations_info)
        for _ in range(0, self._ej3tsolvers_cant):
            self._channel.basic_publish(
                exchange='',
                routing_key=EJ3STATIONS,
                body=data,
                properties=pika.BasicProperties(
                delivery_mode = 2, # make message persistent
            ))

    def _send_eof_confirm(self):
        json_eof = json.dumps({
            "EjSolver": self._EjSolver,
            "eof": STATIONS
        })
        self._send(json_eof)

    def _callback_trips(self, ch, method, properties, body):
        body = body.decode("utf-8")
        self._ej3tsolvers_cant -= 1
        trips = eval(body)
        for k, v in trips.items():
            values = v.split(",")
            self._montreal_stations[k].add_trip(values[0], values[1])
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if self._ej3tsolvers_cant == 0:
            self._send_results()
            self._exit()

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
        for key, value in self._montreal_stations.items():
            avg_km = value.get_average_km()
            if avg_km > 6:
                results[key] = avg_km
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

class MontrealStation:
    def __init__(self, latitude, longitude):
        self._latitude = float(latitude)
        self._longitude = float(longitude)

        self._trips = 0
        self._total_km_to_come = 0.0

    def add_trip(self, n, km):
        self._trips += n
        self._total_km_to_come += km
    
    def get_average_km(self):
        try:
            return self._total_km_to_come / self._trips
        except ZeroDivisionError:
            return 0.0
