.PHONY: build dev test

base:
	docker build -t redis-py-base docker/base

dev: base
	docker-compose up -d --build

test: dev
	docker-compose run --rm test util/wait-for-it.sh master:6379 -- tox -- --redis-url=redis://master:6379/9

clean:
	docker-compose stop
	docker-compose rm
