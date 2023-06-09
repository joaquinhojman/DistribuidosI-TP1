#!/usr/bin/env python3

import logging
from configparser import ConfigParser
import signal
from common.FileReader import FileReader
import os

def initialize_config():
    """ Parse env variables or config file to find program config params
    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["ip"] = os.getenv('SERVER_IP', config["DEFAULT"]["SERVER_IP"])
        config_params["port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["rows_per_batch"] = int(os.getenv('ROWS_PER_BATCH', config["DEFAULT"]["ROWS_PER_BATCH"]))
        config_params["wait_time"] = float(os.getenv('WAIT_TIME', config["DEFAULT"]["WAIT_TIME"]))
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def main():
    config_params = initialize_config()
    ip = config_params["ip"]
    port = config_params["port"]
    logging_level = config_params["logging_level"]
    rows_per_batch = config_params["rows_per_batch"]
    wait_time = config_params["wait_time"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    #logging.debug(f"action: config | result: success | port: {port} | "
    #              f"listen_backlog: {listen_backlog} | logging_level: {logging_level}")

    # Initialize client
    file_reader = FileReader(port, ip, rows_per_batch, wait_time)
    signal.signal(signal.SIGTERM, file_reader._sigterm_handler)
    file_reader.run()

def initialize_log(logging_level):
    """
    Python custom logging initialization
    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

if __name__ == "__main__":
    main()
