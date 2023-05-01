FROM python:3.9.7-slim
RUN pip install pika
COPY ej_solver /
ENTRYPOINT ["/bin/sh"]
