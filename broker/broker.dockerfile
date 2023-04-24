FROM rabbitmq-python-base:0.0.1

COPY broker /
ENTRYPOINT ["/bin/sh"]
