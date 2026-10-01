BOOTSTRAP ?= localhost:9092
RAW ?= ventas.raw
TOTALES ?= ventas.totales

.PHONY: help sync up down topics producer replay pipeline consumer test lint smoke clean

help:
	@echo "Objetivos disponibles:"
	@echo "  make sync       Instalar dependencias con uv"
	@echo "  make up         Levantar Kafka y crear topicos (docker compose)"
	@echo "  make down       Detener y limpiar Kafka"
	@echo "  make producer   Producir eventos sinteticos a $(RAW)"
	@echo "  make replay     Reproducir data/eventos-ejemplo.jsonl a $(RAW)"
	@echo "  make pipeline   Ejecutar el pipeline Beam Kafka -> Kafka (streaming)"
	@echo "  make consumer   Consumir y materializar $(TOTALES)"
	@echo "  make test       Correr la suite de pruebas"
	@echo "  make lint       Correr ruff"
	@echo "  make smoke      Smoke test end-to-end en modo archivo (sin Kafka)"
	@echo "  make clean      Borrar salidas locales"

sync:
	uv sync

up:
	docker compose up -d
	@echo "Kafka arriba. Topicos creados por init-topics."

down:
	docker compose down -v

topics:
	docker compose up init-topics

producer:
	uv run python src/producer.py --mode synthetic --bootstrap $(BOOTSTRAP) --topic $(RAW) --rate 5 --count 200

replay:
	uv run python src/producer.py --mode replay --bootstrap $(BOOTSTRAP) --topic $(RAW) --input data/eventos-ejemplo.jsonl

pipeline:
	uv run python src/pipeline.py --source kafka --sink kafka \
		--bootstrap $(BOOTSTRAP) --input-topic $(RAW) --output-topic $(TOTALES) \
		--window-seconds 60 --allowed-lateness 120 --early-seconds 10

consumer:
	uv run python src/consumer.py --bootstrap $(BOOTSTRAP) --topic $(TOTALES) --from-beginning

test:
	uv run pytest -q

lint:
	uv run ruff check src tests

smoke:
	bash scripts/smoke_test.sh

clean:
	rm -rf out
