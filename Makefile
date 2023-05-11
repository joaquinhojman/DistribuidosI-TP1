SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/client github.com/7574-sistemas-distribuidos/docker-compose-init/client
.PHONY: build

docker-image:
	docker build -f ./entry_point/Dockerfile -t "entry_point:latest" .
	docker build -f ./file_reader/Dockerfile -t "file_reader:latest" .
	docker build -f ./broker/broker.dockerfile -t "broker:latest" .
	docker build -f ./filter/filter.dockerfile -t "filter:latest" .
	docker build -f ./ej_solver/ej_solver.dockerfile -t "ej_solver:latest" .
	docker build -f ./eof_listener/Dockerfile -t "eof_listener:latest" .
	docker build -f ./eof_trips_listener/Dockerfile -t "eof_trips_listener:latest" .
	docker build -f ./ejt_solver/ejt_solver.dockerfile -t "ejt_solver:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
