
import socket
import logging
#import pika

from protocol.protocol import Protocol
from Data import Data

WEATHER = "weathers"
STATIONS = "stations"
TRIPS = "trips"

class EntryPoint:
    def __init__(self, port, listen_backlog):
        # Initialize entrypoint socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
    
        self._sigterm_received = False
        self._client_socket = None
        self._protocol = None
        self._channel = None

        #self._create_RabbitMQ_Connection()

    def _create_RabbitMQ_Connection(self):
        # Create RabbitMQ communication channel
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()

        channel.queue_declare(queue=WEATHER, durable=True)
        channel.queue_declare(queue=STATIONS, durable=True)
        channel.queue_declare(queue=TRIPS, durable=True)

        self._channel = channel

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        self._sigterm_received = True
        try:
            self._server_socket.close()
        except:
            pass
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        try:
            logging.info(f'action: run | result: in_progress')
            while not self._sigterm_received:
                client_socket = self._accept_new_connection(self._server_socket)
                if client_socket is None: continue
                self._client_socket = client_socket
                self._run()
        except Exception as e:
            logging.error(f'action: run | result: fail | error: {e}')
            self._sigterm_handler()
            return

    def _accept_new_connection(self, server_socket: socket):
        logging.info(f'action: accept_connections | result: in_progress')
        try:
            server_socket.settimeout(self._timeout)
            c, addr = server_socket.accept()
            server_socket.settimeout(None)
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
        
        self._send_results()

    def _receive_data(self, topic):
        logging.info(f'action: receive_data | topic: {topic} | result: in_progress')
        _topic = self._receive_topic()
        if _topic is None or _topic is not topic:
            logging.error(f'action: receive_topic | result: fail | topic: {_topic}')
            return False

        try: 
            while True:
                data_recv = self._protocol.receive()
                data = Data(data_recv)
                if data.topic != topic: return None
                if data.eof == True: 
                    break
                
                #self._send_data_to_queue(topic, data.data)
                
                self._protocol.send_ack(True)

            self._send_eofs(topic)
            #expects solvers confirmation to send eof ack
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
            self._protocol.send_ack(True)
            return topic
        except Exception as e:
            return None

    def _send_data_to_queue(self, queue, data):
        self._channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=data,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
        ))
    
    def _send_eofs(self, topic):
        pass

    def _send_results(self):
        pass
