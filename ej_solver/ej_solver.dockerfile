FROM python:3.9.7-slim
RUN pip install pika
RUN pip install haversine
COPY ej_solver /
ENTRYPOINT ["/bin/sh"]
