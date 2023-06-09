import logging
from configparser import ConfigParser
import os
import signal
from common.Broker import Broker
from common.middleware import BrokerMiddleware

def initialize_config():
    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["weather"] = os.getenv('WEATHER', config["DEFAULT"]["WEATHER"])
        config_params["stations"] = os.getenv('STATIONS', config["DEFAULT"]["STATIONS"])
        config_params["trips"] = os.getenv('TRIPS', config["DEFAULT"]["TRIPS"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    weather = config_params["weather"]
    stations = config_params["stations"]
    trips = config_params["trips"]
    broker = os.getenv('BROKER_TYPE', "")
    broker_number = os.getenv('BROKER_ID', "")
    try:
        middleware = BrokerMiddleware(broker)
    except Exception as e:
        logging.error(f"action: config | result: error | broker: {broker} | broker_number: {broker_number} | logging_level: {logging_level} | error: {e}")
        exit(0)

    initialize_log(logging_level)
    logging.info(f"action: config | result: success | broker: {broker} | broker_number: {broker_number} | logging_level: {logging_level}")

    broker = Broker(broker, broker_number, weather, stations, trips, middleware)
    signal.signal(signal.SIGTERM, broker._sigterm_handler)
    broker.run()

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