from common.Middleware import Middleware

RESULTS = "results"

class EjSolverMiddleware(Middleware):
    def __init__(self, EjSolver):
        super().__init__()
        self._EjSolver = EjSolver
        
        super().queue_declare(queue=self._EjSolver, durable=True)
        super().queue_declare(queue=RESULTS, durable=True)

    def recv_data(self, callback):
        super().basic_qos(prefetch_count=1)
        super().recv_message(self._EjSolver, lambda ch, method, properties, body: callback(body.decode("utf-8"), method))
        super().start_consuming()

    def finished_message_processing(self, method):
        super().send_ack(method.delivery_tag)

    def send_data(self, data):
        super().send_message(queue=RESULTS, data=data)

    def close(self):
        super().stop_consuming()
        super().close()
