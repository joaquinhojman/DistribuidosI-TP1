from common.Middleware import Middleware

EOFTRIPSLISTENER = "eoftripslistener"
EJ1TRIPSSOLVER = "ej1tripssolver"
EJ2TRIPSSOLVER = "ej2tripssolver"
EJ3TRIPSSOLVER = "ej3tripssolver"
WEATHER_EJ1_EXCHANGE = "weather_ej1_exchange"
STATIONS_EJ2_EXCHANGE = "stations_ej2_exchange"
STATIONS_EJ3_EXCHANGE = "stations_ej3_exchange"
WE1="weatherej1"
SE2="stationsej2"
TE2="tripsej2"
SE3="stationsej3"
TE3="tripsej3"

class FilterMiddleware(Middleware):
    def __init__(self, filter_type):
        self._filter_type = filter_type
        super().__init__()
    
        super().queue_declare(queue=self._filter_type, durable=True)
        super().queue_declare(queue=EOFTRIPSLISTENER, durable=True)

    def recv_weathers_for_ej1(self, callback, cant_ej_trips_solver):
        self._recv_messages(callback, cant_ej_trips_solver, WEATHER_EJ1_EXCHANGE)

    def recv_stations_for_ej2(self, callback, cant_ej_trips_solver):
        self._recv_messages(callback, cant_ej_trips_solver, STATIONS_EJ2_EXCHANGE)

    def recv_stations_for_ej3(self, callback, cant_ej_trips_solver):
        self._recv_messages(callback, cant_ej_trips_solver, STATIONS_EJ3_EXCHANGE)

    def _recv_messages(self, callback, cant_ej_trips_solver, exchange):
        super().exchange_declare(exchange=exchange, exchange_type='fanout')
        self._create_queues_for_exchange(exchange=exchange, type=self._filter_type, ejtripstsolvercant=cant_ej_trips_solver)
        super().basic_qos(prefetch_count=1)
        super().recv_message(self._filter_type, lambda ch, method, properties, body: callback(body.decode("utf-8"), method))
        super().start_consuming()

    def recv_trips_for_ej2(self, callback):
        super().queue_declare(queue=EJ2TRIPSSOLVER, durable=True)
        self._recv_trips(callback, self._filter_type)

    def recv_trips_for_ej3(self, callback):
        super().queue_declare(queue=EJ3TRIPSSOLVER, durable=True)
        self._recv_trips(callback, self._filter_type)

    def _recv_trips(self, callback, queue):
        super().queue_declare(queue=queue, durable=True)
        super().basic_qos(prefetch_count=1)
        super().recv_message(queue, lambda ch, method, properties, body: callback(body.decode("utf-8"), method))
        super().start_consuming()

    def _create_queues_for_exchange(self, exchange, type, ejtripstsolvercant):
        for i in range(1, ejtripstsolvercant + 1):
            queue_name = f'{type}_{i}'
            super().queue_declare(queue=queue_name, durable=True)
            super().queue_bind(exchange=exchange, queue=queue_name)

    def send_weather_ej1(self, data):
        super().send_to_exchange(WEATHER_EJ1_EXCHANGE, data)

    def send_stations_ej2(self, data):
        super().send_to_exchange(STATIONS_EJ2_EXCHANGE, data)

    def send_stations_ej3(self, data):
        super().send_to_exchange(STATIONS_EJ3_EXCHANGE, data)

    def send_trips_ej2(self, data):
        super().send_message(EJ2TRIPSSOLVER, data)

    def send_trips_ej3(self, data):
        super().send_message(EJ3TRIPSSOLVER, data)

    def send_eof_static_data(self, data):
        if self._filter_type == WE1:
            super().send_to_exchange(WEATHER_EJ1_EXCHANGE, data)
        elif self._filter_type == SE2:
            super().send_to_exchange(STATIONS_EJ2_EXCHANGE, data)
        elif self._filter_type == SE3:
            super().send_to_exchange(STATIONS_EJ3_EXCHANGE, data)

    def send_eof_to_trips_listener(self, data):
        super().send_message(EOFTRIPSLISTENER, data)

    def finished_message_processing(self, method):
        super().send_ack(method.delivery_tag)

    def close(self):
        super().stop_consuming()
        super().close()
