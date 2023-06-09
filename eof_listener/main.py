import logging
from configparser import ConfigParser
import os
import signal
from common.EofListener import EofListener
from common.middleware import EofListenerMiddleware

def initialize_config():
    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    try:
        middleware = EofListenerMiddleware()
    except Exception as e:
        logging.error(f"action: config | result: error | logging_level: {logging_level} | error: {e}")
        exit(0)

    initialize_log(logging_level)
    logging.debug(f"action: config | result: success | logging_level: {logging_level}")

    eof_listener = EofListener(middleware)
    signal.signal(signal.SIGTERM, eof_listener._sigterm_handler)
    eof_listener.run()

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
