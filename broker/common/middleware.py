from common.Middleware import Middleware

EOFLISTENER = "eoflistener"
EOFTRIPSLISTENER = "eoftripslistener"
EJ1TRIPSSOLVER = "ej1tripssolver"
EJ2SOLVER = "ej2solver"
EJ3SOLVER = "ej3solver"
WEATHEREJ1FILTER = "weatherej1"
STATIONSEJ2FILTER = "stationsej2"
TRIPSEJ2FILTER = "tripsej2"
STATIONSEJ3FILTER = "stationsej3"
TRIPSEJ3FILTER = "tripsej3"

class BrokerMiddleware(Middleware):
    def __init__(self, broker_type):
        self._broker_type = broker_type
        super().__init__()
    
        super().queue_declare(queue=self._broker_type, durable=True)
        super().queue_declare(queue=EOFLISTENER, durable=True)
        super().queue_declare(queue=EOFTRIPSLISTENER, durable=True)

    def recv_weather(self, callback_weather):
        super().queue_declare(queue=WEATHEREJ1FILTER, durable=True)
        super().basic_qos(prefetch_count=1)
        super().recv_message(self._broker_type, lambda ch, method, properties, body: callback_weather(body.decode("utf-8"), method))
        super().start_consuming()

    def recv_stations(self, callback_stations):
        super().queue_declare(queue=STATIONSEJ2FILTER, durable=True)
        super().queue_declare(queue=STATIONSEJ3FILTER, durable=True)
        super().basic_qos(prefetch_count=1)
        super().recv_message(self._broker_type, lambda ch, method, properties, body: callback_stations(body.decode("utf-8"), method))
        super().start_consuming()

    def recv_trips(self, callback_trips):
        super().queue_declare(queue=EJ1TRIPSSOLVER, durable=True)
        super().queue_declare(queue=TRIPSEJ2FILTER, durable=True)
        super().queue_declare(queue=TRIPSEJ3FILTER, durable=True)
        super().basic_qos(prefetch_count=1)
        super().recv_message(self._broker_type, lambda ch, method, properties, body: callback_trips(body.decode("utf-8"), method))
        super().start_consuming()

    def send_weather(self, weather):
        self._send_data_to_queue(WEATHEREJ1FILTER, weather)

    def send_station(self, station_for_ej2solver, station_for_ej3filter):
        self._send_data_to_queue(STATIONSEJ2FILTER, station_for_ej2solver)
        self._send_data_to_queue(STATIONSEJ3FILTER, station_for_ej3filter)

    def send_trips(self, trips_for_ej1tripssolver, trips_for_ej2filter, trips_for_ej3solver):
        self._send_data_to_queue(EJ1TRIPSSOLVER, trips_for_ej1tripssolver)
        self._send_data_to_queue(TRIPSEJ2FILTER, trips_for_ej2filter)
        self._send_data_to_queue(TRIPSEJ3FILTER, trips_for_ej3solver)

    def finished_message_processing(self, method):
        super().send_ack(method.delivery_tag)

    def _send_data_to_queue(self, queue, data):
        super().send_message(queue=queue, data=data)

    def send_eof_to_eof_listener(self, data):
        super().send_message(queue=EOFLISTENER, data=data)

    def send_eof_to_eof_trips_listener(self, data):
        super().send_message(queue=EOFTRIPSLISTENER, data=data)

    def close(self):
        super().stop_consuming()
        super().close()
