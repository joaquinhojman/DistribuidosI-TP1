import pika

class Middleware:
    def __init__(self) -> None:
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
    
    def basic_qos(self, prefetch_count):
        self.channel.basic_qos(prefetch_count=prefetch_count)

    def queue_declare(self, queue, durable=False, exclusive=False):
        self.channel.queue_declare(queue=queue, durable=durable, exclusive=exclusive)

    def exchange_declare(self, exchange, exchange_type):
        self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)

    def queue_bind(self, exchange, queue):
        self.channel.queue_bind(exchange=exchange, queue=queue)

    def start_consuming(self):
        self.channel.start_consuming()

    def send_message(self, queue, data):
        self.channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=data,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
        ))

    def send_to_exchange(self, exchange, routing_key, message):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key='',
            body=message,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
        ))

    def stop_recv_message(self, consumer_tag):
        self.channel.basic_cancel(consumer_tag=consumer_tag)

    def recv_message(self, queue, callback, autoack=False):
        return self.channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=autoack)
    
    def send_ack(self, method):
        self.channel.basic_ack(delivery_tag=method)

    def stop_consuming(self):
        self.channel.stop_consuming()

    def close(self):
        self.channel.close()
