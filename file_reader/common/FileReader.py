import logging
import os
import socket

from protocol.protocol import Protocol

class FileReader:
    def __init__(self, port, ip, rows_per_batch):
        self._ip = ip
        self._port = port
        self._rows_per_batch = rows_per_batch
    
        self._server = None
        self._protocol = None
        self._f = None
    
    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        self._close_connection()
        if self._f is not None:
            self._f.close()
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        self._make_connection()

        #self._send_data("data/test/weather.csv", send_topic=True, send_eof=True)
        #self._send_data("data/test/stations.csv", send_topic=True, send_eof=True)
        #self._send_data("data/test/trips.csv", send_topic=True, send_eof=True)
        
        self._send_data("data/montreal/weather.csv", send_topic=True)
        self._send_data("data/toronto/weather.csv")
        self._send_data("data/washington/weather.csv", send_eof=True)
        self._send_data("data/montreal/stations.csv", send_topic=True)
        self._send_data("data/toronto/stations.csv")
        self._send_data("data/washington/stations.csv", send_eof=True)
        self._send_data("data/montreal/trips.csv", send_topic=True)
        self._send_data("data/toronto/trips.csv")
        self._send_data("data/washington/trips.csv", send_eof=True)

        self._receive_results()

        self._close_connection()

    def _make_connection(self):
        logging.info(f'action: make_connection | result: in_progress | ip: {self._ip} | port: {self._port}')
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self._ip, self._port))
        self._protocol = Protocol(self._socket)

    def _send_data(self, file_path, send_topic=False, send_eof=False):
        try:
            logging.info(f'action: send data | data: {file_path} | result: in_progress')
            data_type = os.path.splitext(file_path)[0].split('/')[2]
            city_name = os.path.splitext(file_path)[0].split('/')[1]

            if (send_topic): self._send_topic(data_type)

            self._f = open(file_path, 'r')
            row_header = self._f.readline().strip().split(',')
            eof = False
            while not eof:
                data, eof = self._get_data(row_header, data_type, city_name)
                self._send(data)
            self._f.close()

            if (send_eof): self._send_eof(data_type)
            logging.info(f'action: send data | data: {file_path} | result: success')
        except Exception as e:
            logging.error(f'action: send data | data: {file_path} | result: failed | error: {e}')
            raise e

    def _send_topic(self, data_type):
        self._send(self._get_topic_packet(data_type))

    def _get_topic_packet(self, data_type):
        return data_type + ";0"

    def _get_data(self, row_header: list, data_type: str, city_name: str):
        data = ""
        eof = False
        for _i in range(self._rows_per_batch):
            line = self._f.readline().strip()
            if not line: #End of file?
                eof = True
                break
            json = self._make_json(city_name, row_header, line.split(','))
            data += json + '\n'
        data = data[:-1] # remove last comma
        
        if eof == False: # could happen that next line is end of file
            x = self._f.tell()
            line = self._f.readline()
            self._f.seek(x) #return to previous position
            if not line:
                eof = True

        return data_type + ";0" + ";" + data, eof

    def _make_json(self, city_name, row_header, line_data):
        row = "{"
        row += f'"city":"{city_name}",'
        for i in range(len(row_header)):
            row += f'"{row_header[i]}":"{line_data[i]}",'
        row = row[:-1] + "}"
        return row

    def _send_eof(self, data_type):
        self._send(self._get_eof_packet(data_type), True)

    def _get_eof_packet(self, data_type):
        return data_type + ";1"

    def _send(self, data, expect_ack=False):
        self._protocol.send(data)
        if not expect_ack: return
        ack = self._protocol.receive_ack()
        if ack == False:
            raise Exception("received ack was False")
        if not ack:
            raise OSError("Socket connection broken during send data")
    
    def _receive_results(self):
        data = self._protocol.receive()
        self._protocol.send_ack(True)
        logging.info(f'action: Receive results | result: success | data: {data}')

    def _close_connection(self):
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError:
                pass
