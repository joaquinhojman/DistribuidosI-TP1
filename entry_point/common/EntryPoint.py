
import json
import os
import socket
import logging
from time import sleep
import pika

from protocol.protocol import Protocol
from common.Data import Data

WEATHER = "weather"
STATIONS = "stations"
TRIPS = "trips"
RESULTS = "results"
EJ1SOLVER = "ej1solver"
EJ2SOLVER = "ej2solver"
EJ3SOLVER = "ej3solver"

class EntryPoint:
    def __init__(self, port, listen_backlog):
        # Initialize entrypoint socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)

        self._cant_brokers = {
            WEATHER: int(os.getenv('WBRKCANT', "")),
            STATIONS: int(os.getenv('SBRKCANT', "")),
            TRIPS: int(os.getenv('TBRKCANT', ""))
        }

        self._actual_topic = None
        self._solvers_confirmated = {
            EJ1SOLVER: False,
            EJ2SOLVER: False,
            EJ3SOLVER: False
        }
        self._results = {
            EJ1SOLVER: None,
            EJ2SOLVER: None,
            EJ3SOLVER: None
        }
    
        self._sigterm_received = False
        self._client_socket = None
        self._protocol = None
        self._channel = None

        self._create_RabbitMQ_Connection()

    def _create_RabbitMQ_Connection(self):
        logging.info(f'action: create rabbitmq connections | result: in_progress')
        while self._channel is None:
            try: 
                # Create RabbitMQ communication channel
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq'))
                channel = connection.channel()

                channel.queue_declare(queue=WEATHER, durable=True)
                channel.queue_declare(queue=STATIONS, durable=True)
                channel.queue_declare(queue=TRIPS, durable=True)
                channel.queue_declare(queue=RESULTS, durable=True)

                self._channel = channel
            except Exception as e:
                sleep(15)
        logging.info(f'action: create rabbitmq connections | result: success')

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        self._sigterm_received = True
        try:
            self._server_socket.close()
            self._channel.close()
        except:
            pass
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        try:
            logging.info(f'action: run | result: in_progress')            
            client_socket = self._accept_new_connection(self._server_socket)
            if client_socket is None: return
            self._client_socket = client_socket
            self._run()
            self._close_connection()
        except Exception as e:
            logging.error(f'action: run | result: fail | error: {e}')
            self._sigterm_handler()
            return

    def _accept_new_connection(self, server_socket: socket):
        logging.info(f'action: accept_connections | result: in_progress')
        try:
            c, addr = server_socket.accept()
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except OSError as e:
            return None

    def _run(self):
        self._protocol = Protocol(self._client_socket)
        
        res = self._receive_data(WEATHER)
        if res == False: return
        res = self._receive_data(STATIONS)
        if res == False: return
        res = self._receive_data(TRIPS)
        if res == False: return
        
        ack = self._send_results()
        logging.info(f'action: run | result: success | ack: {ack}')

    def _receive_data(self, topic):
        logging.info(f'action: receive_data | topic: {topic} | result: in_progress')
        _topic = self._receive_topic()
        if _topic is None or _topic != topic:
            logging.error(f'action: receive_topic | result: fail | topic: {_topic} | expected: {topic}')
            return False
        self._actual_topic = topic

        try: 
            while True:
                data_recv = self._protocol.receive()
                data = Data(data_recv)
                if data.topic != topic: return None
                if data.eof == True: break
                
                self._send_data_to_queue(topic, data.data)
                
                #self._protocol.send_ack(True)

            self._send_eofs()
            self._expect_solvers_confirmation()
            self._protocol.send_ack(True)
            self._actual_topic = None
        except Exception as e:
            try:
                self._protocol.send_ack(False)
            except OSError as _e:
                logging.error(f'action: receive_message | result: fail | error: {_e}')
            logging.error(f'action: receive_data | topic: {topic} | result: fail | error: {e}')
            return False

        logging.info(f'action: receive_data | topic: {topic} | result: success')
        return True

    def _receive_topic(self):
        logging.info(f'action: receive_topic | result: in_progress')
        try:
            data = self._protocol.receive()
            topic = data.split(';')[0]
            logging.info(f'action: receive_topic | result: success | topic: {topic}')
            #self._protocol.send_ack(True)
            return topic
        except Exception as e:
            logging.error(f'action: receive_topic | result: fail | error: {e}')
            return None

    def _send_eofs(self):
        logging.info(f'action: receive_data | eof received | topic: {self._actual_topic}')
        for _ in range(self._cant_brokers[self._actual_topic]):
            self._send_data_to_queue(self._actual_topic, "EOF")

    def _send_data_to_queue(self, queue, data):
        self._channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=data,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
        ))

    def _expect_solvers_confirmation(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue='results', on_message_callback=self._callback)
        self._channel.start_consuming()

    def _callback(self, ch, method, properties, body):
        logging.info(f'action: _callback | result: in_progress')
        finished = False
        eof = EOF(body.decode('utf-8'))
        if eof.eof != self._actual_topic:
            logging.error(f'action: _callback | result: fail | error: eof.eof != self._actual_topic')
            return
        self._solvers_confirmated[eof.EjSolver] = True
        self._results[eof.EjSolver] = eof.results
        if self._all_solvers_confirmated():
            self._reset_solvers_confirmated_dict()
            finished = True
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if finished: self._channel.stop_consuming()

    def _send_results(self):
        self._protocol.send(str(self._results))
        ack = self._protocol.receive_ack()
        return ack

    def _all_solvers_confirmated(self):
        logging.info(f'action: _all_solvers_confirmated | result: in_progress | {self._solvers_confirmated} | {self._actual_topic}')
        if self._actual_topic == WEATHER:
            return self._solvers_confirmated[EJ1SOLVER] == True
        elif self._actual_topic == STATIONS:
            return self._solvers_confirmated[EJ2SOLVER] == True and self._solvers_confirmated[EJ3SOLVER] == True
        elif self._actual_topic == TRIPS:
            return all(self._solvers_confirmated.values())
        return False

    def _reset_solvers_confirmated_dict(self):
        for key in self._solvers_confirmated:
            self._solvers_confirmated[key] = False

    def _close_connection(self):
        if self._client_socket is not None:
            self._client_socket.close()
        if self._server_socket is not None:
            self._server_socket.close()
        if self._channel is not None:
            self._channel.close()

class EOF:
    def __init__(self, data):
        data = json.loads(data)
        self.EjSolver = data['EjSolver']
        self.eof = data["eof"]
        self.results = data["results"] if self.eof == "trips" else None
