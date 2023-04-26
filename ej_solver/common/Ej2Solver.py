import logging
import os
from time import sleep
import pika

class Ej2Solver:
    def __init__(self, EjSolver, channel):
        self._EjSolver = EjSolver
        self._channel = channel

    def run(self):
        logging.info(f'action: run_Ej2Solver | result: in_progress')
        self._channel.basic_consume(queue=self.EjSolver, on_message_callback=self._callback)

    def _callback():
        pass
