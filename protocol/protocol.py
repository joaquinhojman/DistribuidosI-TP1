from configparser import ConfigParser
import logging
import os
import socket

def _initialize_config():
    config = ConfigParser(os.environ)
    config.read("protocol.ini")

    config_params = {}
    try:
        config_params["cant_bytes_for_len"] = int(os.getenv('CANT_BYTES_FOR_LEN', config["DEFAULT"]["CANT_BYTES_FOR_LEN"]))
        config_params["cant_bytes_for_ack"] = int(os.getenv('CANT_BYTES_FOR_ACK', config["DEFAULT"]["CANT_BYTES_FOR_ACK"]))
        config_params["max_cant_bytes_for_packet"] = int(os.getenv('MAX_CANT_BYTES_FOR_PACKET', config["DEFAULT"]["MAX_CANT_BYTES_FOR_PACKET"]))
        config_params["success"] = int(os.getenv('SUCCESS', config["DEFAULT"]["SUCCESS"]))
        config_params["error"] = int(os.getenv('ERROR', config["DEFAULT"]["ERROR"]))
        config_params["cant_bytes_for_eof"] = int(os.getenv('CANT_BYTES_FOR_EOF', config["DEFAULT"]["CANT_BYTES_FOR_EOF"]))
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))
    
    return config_params

class Protocol:
    def __init__(self, socket):
        self._socket = socket
        config_params = _initialize_config()
        self._cant_bytes_for_len = config_params["cant_bytes_for_len"]
        self._max_cant_bytes_for_packet = config_params["max_cant_bytes_for_packet"]
        self._success_ack = config_params["success"]
        self._error_ack = config_params["error"]
        self._cant_bytes_for_ack = config_params["cant_bytes_for_ack"]
        self._eof = config_params["success"]
        self._not_eof = config_params["error"]
        self._cant_bytes_for_eof = config_params["cant_bytes_for_eof"]
