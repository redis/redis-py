.PHONY: base clean dev test

base:
		docker build -t redis-py-base docker/base

dev:	base
		docker-compose up -d --build

test:	dev
		docker-compose run --rm test /redis-py/docker-entry.sh

clean:
		docker-compose stop
		docker-compose rm -f
