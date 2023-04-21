import logging
import os
import socket

from protocol.protocol import Protocol

class FileReader:
    def __init__(self, port, ip, rows_per_batch):
        self._rows_per_batch = rows_per_batch
        self._ip = ip
        self._port = port
        self._socket = None
        
    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        self._close_connection()
        self._f.close()
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        pass

    def _close_connection(self):
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError:
                pass
