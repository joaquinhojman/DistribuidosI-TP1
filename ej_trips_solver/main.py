import logging
from configparser import ConfigParser
import os
import signal
from common.EjTripsSolver import EjTripsSolver

def initialize_config():
    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["ej1tripssolver"] = os.getenv('EJ1TRIPSSOLVER', config["DEFAULT"]["EJ1TRIPSSOLVER"])
        config_params["ej2tripssolver"] = os.getenv('EJ2TRIPSSOLVER', config["DEFAULT"]["EJ2TRIPSSOLVER"])
        config_params["ej3tripssolver"] = os.getenv('EJ3TRIPSSOLVER', config["DEFAULT"]["EJ3TRIPSSOLVER"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    ej1tripssolver = config_params["ej1tripssolver"]
    ej2tripssolver = config_params["ej2tripssolver"]
    ej3tripssolver = config_params["ej3tripssolver"]
    ejtripssolver = os.getenv('EJTRIPSSOLVER', "")

    initialize_log(logging_level)
    logging.info(f"action: config | result: success | ejtripssolver: {ejtripssolver} | logging_level: {logging_level}")


    ej_solver = EjTripsSolver(ejtripssolver, ej1tripssolver, ej2tripssolver, ej3tripssolver)
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
