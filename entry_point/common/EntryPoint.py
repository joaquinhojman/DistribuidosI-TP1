
import socket
import logging


class EntryPoint:
    def __init__(self, port, listen_backlog):
        # Initialize entrypoint socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
    
    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        try:
            self._server_socket.close()
        except:
            pass
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        pass