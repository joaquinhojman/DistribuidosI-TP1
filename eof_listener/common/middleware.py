from common.Middleware import Middleware

EOFLISTENER = "eoflistener"
WEATHEREJ1FILTER = "weatherej1"
STATIONSEJ2FILTER = "stationsej2"
TRIPSEJ2FILTER = "tripsej2"
STATIONSEJ3FILTER = "stationsej3"
TRIPSEJ3FILTER = "tripsej3"
EOF = "eof"

class EofListenerMiddleware(Middleware):
    def __init__(self):
        super().__init__()

        super().queue_declare(queue=EOFLISTENER, durable=True)
        super().queue_declare(queue=WEATHEREJ1FILTER, durable=True)
        super().queue_declare(queue=STATIONSEJ2FILTER, durable=True)
        super().queue_declare(queue=TRIPSEJ2FILTER, durable=True)
        super().queue_declare(queue=STATIONSEJ3FILTER, durable=True)
        super().queue_declare(queue=TRIPSEJ3FILTER, durable=True)

    def recv_eofs(self, callback):
        super().basic_qos(prefetch_count=1)
        super().recv_message(EOFLISTENER, lambda ch, method, properties, body: callback(body.decode("utf-8"), method))
        super().start_consuming()

    def send_eof_to_filter(self, filter, message):
        super().send_message(queue=filter, data=message)
    
    def finished_message_processing(self, method):
        super().send_ack(method.delivery_tag)

    def close(self):
        super().stop_consuming()
        super().close()
