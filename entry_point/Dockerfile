FROM python:3.9.7-slim
RUN pip install pika
COPY ../protocol/ ../protocol/
COPY ../common/ ../common/
COPY entry_point /
ENTRYPOINT ["/bin/sh"]
