from common.Middleware import Middleware

EJ1TRIPSSOLVER="ej1tripssolver"
EJ2TRIPSSOLVER="ej2tripssolver"
EJ3TRIPSSOLVER="ej3tripssolver"
EJ1SOLVER = "ej1solver"
EJ2SOLVER = "ej2solver"
EJ3SOLVER = "ej3solver"
WEATHEREJ1FILTER = "weatherej1"
STATIONSEJ2FILTER = "stationsej2"
STATIONSEJ3FILTER = "stationsej3"
WEATHER_EJ1_EXCHANGE = "weather_ej1_exchange"
STATIONS_EJ2_EXCHANGE = "stations_ej2_exchange"
STATIONS_EJ3_EXCHANGE = "stations_ej3_exchange"
EOF = "eof"

class EjTripsSolverMiddleware(Middleware):
    def __init__(self, ejtripssolver, id):
        super().__init__()
        self._ej_trips_solver = ejtripssolver
        self._static_data_queue = None
        self._ej_solver_queue = None
        
        if ejtripssolver == EJ1TRIPSSOLVER:
            super().exchange_declare(exchange=WEATHER_EJ1_EXCHANGE, exchange_type='fanout')
            self._static_data_queue = f'{WEATHEREJ1FILTER}_{id}'
            super().queue_declare(queue=self._static_data_queue, durable=True)
            super().queue_bind(exchange=WEATHER_EJ1_EXCHANGE, queue=self._static_data_queue)
            
            self._ej_solver_queue = EJ1SOLVER
            super().queue_declare(queue=self._ej_solver_queue, durable=True)
        elif ejtripssolver == EJ2TRIPSSOLVER:
            super().exchange_declare(exchange=STATIONS_EJ2_EXCHANGE, exchange_type='fanout')
            self._static_data_queue = f'{STATIONSEJ2FILTER}_{id}'
            super().queue_declare(queue=self._static_data_queue, durable=True)
            super().queue_bind(exchange=STATIONS_EJ2_EXCHANGE, queue=self._static_data_queue)
            
            self._ej_solver_queue = EJ2SOLVER
            super().queue_declare(queue=self._ej_solver_queue, durable=True)
        elif ejtripssolver == EJ3TRIPSSOLVER:
            super().exchange_declare(exchange=STATIONS_EJ3_EXCHANGE, exchange_type='fanout')
            self._static_data_queue = f'{STATIONSEJ3FILTER}_{id}'
            super().queue_declare(queue=self._static_data_queue, durable=True)
            super().queue_bind(exchange=STATIONS_EJ3_EXCHANGE, queue=self._static_data_queue)
            
            self._ej_solver_queue = EJ3SOLVER
            super().queue_declare(queue=self._ej_solver_queue, durable=True)

    def recv_static_data(self, callback):
        super().basic_qos(prefetch_count=1)
        super().recv_message(self._static_data_queue, lambda ch, method, properties, body: callback(body.decode("utf-8"), method))
        super().start_consuming()

    def recv_trips(self, callback):
        super().basic_qos(prefetch_count=1)
        super().recv_message(self._ej_trips_solver, lambda ch, method, properties, body: callback(body.decode("utf-8"), method))
        super().start_consuming()

    def send_data(self, data):
        super().send_message(queue=self._ej_solver_queue, data=data)

    def finished_message_processing(self, method):
        super().send_ack(method.delivery_tag)

    def close(self):
        super().stop_consuming()
        super().close()
