import json
import logging
import os
import pika

STATIONS = "stations"
TRIPS = "trips"
EJ2TRIPS = "ej2trips"
EJ2STATIONS = "ej2stations"
RESULTS = "results"

class Ej2Solver:
    def __init__(self, EjSolver, channel):
        self._EjSolver = EjSolver
        self._channel = channel

        self._stations_eof_to_expect = int(os.getenv('SBRKCANT', ""))
        self._ej2tsolvers_cant = int(os.getenv('EJ2TCANT', ""))

        self._stations_name = {}
        self._stations = {}
        channel.queue_declare(queue=EJ2TRIPS, durable=True)
        channel.queue_declare(queue=EJ2STATIONS, durable=True)

    def run(self):
        logging.info(f'action: run_Ej2Solver | result: in_progress')
        self._channel.basic_consume(queue=self._EjSolver, on_message_callback=self._callback)
        self._channel.basic_consume(queue=EJ2TRIPS, on_message_callback=self._callback_trips)

    def _callback(self, ch, method, properties, body):
        finished = False
        body = str(body.decode("utf-8"))
        data = json.loads(body)
        if data["type"] == STATIONS:
            self._stations_name[(data["city"], data["code"], data["yearid"])] = data["name"]
            self._stations[data["name"]] = Station()
        elif data["type"] == "eof":
            finished = self._process_eof()
        else:
            logging.error(f'action: _callback | result: error | error: Invalid data type | data: {data}')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if finished: self._channel.stop_consuming()
    
    def _process_eof(self):
        self._stations_eof_to_expect -= 1
        if self._stations_eof_to_expect == 0:
            self._send_stations_to_ejt2solver()
            self._send_eof_confirm()
            return True
        return False
    
    def _send_stations_to_ejt2solver(self):
        data = str(self._stations_name) + ";" + str(list(self._stations.keys()))
        for _ in range(0, self._ej2tsolvers_cant):
            self._channel.basic_publish(
                exchange='',
                routing_key=EJ2STATIONS,
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
        self._ej2tsolvers_cant -= 1
        trips = eval(body)
        for k, v in trips.items():
            self._stations[k].add_trip("2016", v[0])
            self._stations[k].add_trip("2017", v[1])
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if self._ej2tsolvers_cant == 0:
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
    
    def add_trip(self, year, n):
        if year == "2016":
            self._trips_on_2016 += n
        elif year == "2017":
            self._trips_on_2017 += n
        else:
            logging.error(f'action: add_trip | result: error | error: Invalid year | year: {year}')

    def duplicate_trips(self):
        return (self._trips_on_2016 * 2 <= self._trips_on_2017) and self._trips_on_2017 != 0
