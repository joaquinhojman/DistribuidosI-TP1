from common.Middleware import Middleware

WEATHER = "weather"
STATIONS = "stations"
TRIPS = "trips"
RESULTS = "results"
EOF = "eof"

class EntryPointMiddleware(Middleware):
    def __init__(self):
        super().__init__()
    
        super().queue_declare(queue=WEATHER, durable=True)
        super().queue_declare(queue=STATIONS, durable=True)
        super().queue_declare(queue=TRIPS, durable=True)
        super().queue_declare(queue=RESULTS, durable=True)

    def recv_solvers_confirmation(self, callback):
        super().basic_qos(prefetch_count=1)
        super().recv_message(RESULTS, lambda ch, method, properties, body: callback(body.decode("utf-8"), method))
        super().start_consuming()

    def send_eof_to_topic(self, topic):
        super().send_message(queue=topic, data=EOF)

    def send_data_to_topic(self, topic, data):
        super().send_message(queue=topic, data=data)

    def finished_message_processing(self, method):
        super().send_ack(method.delivery_tag)

    def close(self):
        super().stop_consuming()
        super().close()
