FROM python:3.9.7-slim
RUN pip install pika
COPY broker /
ENTRYPOINT ["/bin/sh"]
