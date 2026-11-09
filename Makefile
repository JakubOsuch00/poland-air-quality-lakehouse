.PHONY: help up down logs lint test

help:
	@echo "make up      - start the local stack"
	@echo "make down    - stop it (volumes are kept)"
	@echo "make lint    - ruff"
	@echo "make test    - unit tests"

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

lint:
	ruff check src tests
	ruff format --check src tests

test:
	pytest

# spark tests need a JVM - this runs them inside the spark image
test-docker:
	docker compose run --rm --no-deps -v ./tests:/opt/aq/tests -v ./pyproject.toml:/opt/aq/pyproject.toml \
		spark-master python -m pytest -p no:cacheprovider
