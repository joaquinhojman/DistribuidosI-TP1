import logging
import os
import socket

from protocol.protocol import Protocol

class FileReader:
    def __init__(self, port, ip, rows_per_batch):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self._ip, self._port))
        self._protocol = Protocol(self._socket)

        self._f = None

        self._rows_per_batch = rows_per_batch
        
    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        self._close_connection()
        if self._f is not None:
            self._f.close()
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        self._send_data("montreal/weather.csv")
        self._send_data("toronto/weather.csv")
        self._send_data("washington/weather.csv", True)
        self._send_data("montreal/stations.csv")
        self._send_data("toronto/stations.csv")
        self._send_data("washington/stations.csv", True)
        self._send_data("montreal/trips.csv")
        self._send_data("toronto/trips.csv")
        self._send_data("washington/trips.csv", True)
        self._receive_results()
        self._close_connection()

    def _send_data(self, file_path, send_eof=False):
        data_type = os.path.splitext(file_path)[0].split('/')[1]
        city_name = os.path.splitext(file_path)[0].split('/')[0]

        self._f = open(self._bets_file, 'r')
        eof = False
        while not eof:
            data, eof = self._get_data(data_type, city_name)
            self._protocol.send(data)
            ack = self._protocol.receive_ack()
            if not ack:
                raise OSError("Socket connection broken during send data")
        self._f.close()

        if (send_eof):
            self._protocol.send(self._get_eof_packet(data_type))
            ack = self._protocol.receive_ack()
            if not ack:
                raise OSError("Socket connection broken during send eof")

    def _get_data(self, data_type, city_name):
        data = []
        eof = False
        for _i in range(self._rows_per_batch):
            line = self._f.readline()
            if not line: #End of file?
                eof = True
                break
            line = self._agencia + "," + line.rstrip()
            data.append(line)
        
        if eof == False: # could happen that next line is end of file
            x = self._f.tell()
            line = self._f.readline()
            self._f.seek(x) #return to previous position
            if not line:
                eof = True

        return data_type + ";0" + ";" + ";".join(data), eof

    def _get_eof_packet(self, data_type):
        return data_type + ";1"
    
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
