import logging
from configparser import ConfigParser
import os
import signal
from common.EjSolver import EjSolver
from common.middleware import EjSolverMiddleware

def initialize_config():
    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["ej1solver"] = os.getenv('EJ1SOLVER', config["DEFAULT"]["EJ1SOLVER"])
        config_params["ej2solver"] = os.getenv('EJ2SOLVER', config["DEFAULT"]["EJ2SOLVER"])
        config_params["ej3solver"] = os.getenv('EJ3SOLVER', config["DEFAULT"]["EJ3SOLVER"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    ej1solver = config_params["ej1solver"]
    ej2solver = config_params["ej2solver"]
    ej3solver = config_params["ej3solver"]
    ejsolver = os.getenv('EJSOLVER', "")
    try:
        middleware = EjSolverMiddleware(ejsolver)
    except Exception as e:
        logging.error(f"action: config | result: error | ejsolver: {ejsolver} | logging_level: {logging_level} | error: {e}")
        exit(0)

    initialize_log(logging_level)
    logging.info(f"action: config | result: success | ejsolver: {ejsolver} | logging_level: {logging_level}")


    ej_solver = EjSolver(ejsolver, ej1solver, ej2solver, ej3solver, middleware)
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
