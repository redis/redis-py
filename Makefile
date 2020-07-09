.PHONY: base

build:
	docker build -t redis-py-base docker/base
	docker-compose down
	docker-compose build

dev:
	docker-compose up -d

test: dev
	docker-compose run test tox --redis-url="redis://master:6379/9"
