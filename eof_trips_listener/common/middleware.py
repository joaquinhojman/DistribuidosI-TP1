from common.Middleware import Middleware

EOFTRIPSLISTENER = "eoftripslistener"
EJ1TRIPSSOLVER = "ej1tripssolver"
EJ2TRIPSSOLVER = "ej2tripssolver"
EJ3TRIPSSOLVER = "ej3tripssolver"

class EofTripsListenerMiddleware(Middleware):
    def __init__(self):
        super().__init__()

        super().queue_declare(queue=EOFTRIPSLISTENER, durable=True)
        super().queue_declare(queue=EJ1TRIPSSOLVER, durable=True)
        super().queue_declare(queue=EJ2TRIPSSOLVER, durable=True)
        super().queue_declare(queue=EJ3TRIPSSOLVER, durable=True)

    def recv_eofs(self, callback):
        super().basic_qos(prefetch_count=1)
        super().recv_message(EOFTRIPSLISTENER, lambda ch, method, properties, body: callback(body.decode("utf-8"), method))
        super().start_consuming()

    def send_eof_to_ej_trips_solver(self, ej_trips_solver, message):
        super().send_message(queue=ej_trips_solver, data=message)
    
    def finished_message_processing(self, method):
        super().send_ack(method.delivery_tag)

    def close(self):
        super().stop_consuming()
        super().close()
