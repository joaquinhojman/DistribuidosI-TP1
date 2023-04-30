import logging
from configparser import ConfigParser
import os
import signal
from common.EjtSolver import EjtSolver

def initialize_config():
    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["ej1tsolver"] = os.getenv('EJ1TSOLVER', config["DEFAULT"]["EJ1TSOLVER"])
        config_params["ej2tsolver"] = os.getenv('EJ2TSOLVER', config["DEFAULT"]["EJ2TSOLVER"])
        config_params["ej3tsolver"] = os.getenv('EJ3TSOLVER', config["DEFAULT"]["EJ3TSOLVER"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    ej1tsolver = config_params["ej1tsolver"]
    ej2tsolver = config_params["ej2tsolver"]
    ej3tsolver = config_params["ej3tsolver"]
    ejtsolver = os.getenv('EJTSOLVER', "")

    initialize_log(logging_level)
    logging.info(f"action: config | result: success | ejtsolver: {ejtsolver} | logging_level: {logging_level}")


    ej_solver = EjtSolver(ejtsolver, ej1tsolver, ej2tsolver, ej3tsolver)
    signal.signal(signal.SIGTERM, ej_solver._sigterm_handler)
    ej_solver.run()

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
