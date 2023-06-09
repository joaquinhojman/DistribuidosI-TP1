import logging
from configparser import ConfigParser
import os
import signal
from common.Filter import Filter
from common.middleware import FilterMiddleware    

def initialize_config():
    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["we1"] = os.getenv('WE1', config["DEFAULT"]["WE1"])
        config_params["se2"] = os.getenv('SE2', config["DEFAULT"]["SE2"])
        config_params["te2"] = os.getenv('TE2', config["DEFAULT"]["TE2"])
        config_params["se3"] = os.getenv('SE3', config["DEFAULT"]["SE3"])
        config_params["te3"] = os.getenv('TE3', config["DEFAULT"]["TE3"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    weather_ej1 = config_params["we1"]
    stations_ej2 = config_params["se2"]
    trips_ej2 = config_params["te2"]
    stations_ej3 = config_params["se3"]
    trips_ej3 = config_params["te3"]
    filter = os.getenv('FILTER_TYPE', "")
    filter_number = os.getenv('FILTER_ID', "")
    try:
        middleware = FilterMiddleware(filter)
    except Exception as e:
        logging.error(f"action: config | result: error | filter: {filter} | filter_number: {filter_number} | logging_level: {logging_level} | error: {e}")
        exit(0)

    initialize_log(logging_level)
    logging.info(f"action: config | result: success | filter: {filter} | filter_number: {filter_number} | logging_level: {logging_level}")

    filter = Filter(filter, filter_number, weather_ej1, stations_ej2, trips_ej2, stations_ej3, trips_ej3, middleware)
    signal.signal(signal.SIGTERM, filter._sigterm_handler)
    filter.run()

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
