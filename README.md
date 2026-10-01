# Ventas de retail en streaming - Proyecto integrador

Pipeline end-to-end con Apache Kafka y Apache Beam que calcula el total de
ventas confirmadas por tienda y por ventana de un minuto, tolerando eventos
duplicados, fuera de orden y tardios.

Fuente -> Kafka (`ventas.raw`) -> Beam (validacion, ventanas, dedup, agregacion)
-> Kafka (`ventas.totales`) -> consumidor (materializacion upsert).

Documento tecnico: `docs/documento-tecnico.md`.
Diagrama: `docs/diagrama-arquitectura.png`.

## Prerrequisitos

- Python 3.11 o 3.12 y [uv](https://docs.astral.sh/uv/).
- Docker y Docker Compose (para el camino Kafka end-to-end).
- El pipeline se ejecuta en el host (no dentro de un contenedor): KafkaIO en
  Python es un transform cross-language y en la primera ejecucion descarga
  imagenes de Beam Java, para lo cual necesita Docker corriendo.

## Instalacion

```bash
uv sync
```

## Camino rapido sin Kafka (smoke test)

Reproduce el dataset de ejemplo por el pipeline en modo archivo y verifica los
resultados. No requiere Docker ni Kafka.

```bash
make smoke
# o directamente:
bash scripts/smoke_test.sh
```

Salida esperada (una fila por tienda y ventana):

```
T-002  2026-07-24T13:00:00+00:00  total=  200000  cantidad=1  pane=ON_TIME
T-014  2026-07-24T13:00:00+00:00  total=  250000  cantidad=3  pane=ON_TIME
T-014  2026-07-24T13:01:00+00:00  total=  120000  cantidad=2  pane=ON_TIME
```

Demuestra deduplicacion (el `event_id` repetido no suma), filtrado por estado
(solo CONFIRMADA), ventana por tiempo de evento y dead-letter (un evento sin
`monto` va a invalidos).

## Camino completo end-to-end con Kafka

Se necesitan cuatro terminales (o usar los objetivos de `make`).

1. Levantar Kafka y crear los topicos:

```bash
docker compose up -d
```

Esto inicia el broker (KRaft) y el servicio `init-topics`, que crea
`ventas.raw`, `ventas.totales` y `ventas.invalidos`. Verificar:

```bash
docker compose logs init-topics
```

2. Ejecutar el pipeline Beam (streaming, Kafka -> Kafka):

```bash
make pipeline
# equivale a:
uv run python src/pipeline.py --source kafka --sink kafka \
    --bootstrap localhost:9092 \
    --input-topic ventas.raw --output-topic ventas.totales \
    --window-seconds 60 --allowed-lateness 120 --early-seconds 10
```

3. Consumir los totales materializados:

```bash
make consumer
# equivale a:
uv run python src/consumer.py --bootstrap localhost:9092 \
    --topic ventas.totales --from-beginning
```

4. Producir eventos (sinteticos, con duplicados/tardios/invalidos inyectados):

```bash
make producer
# equivale a:
uv run python src/producer.py --mode synthetic --bootstrap localhost:9092 \
    --topic ventas.raw --rate 5 --count 200
```

Tambien se puede reproducir el dataset de ejemplo:

```bash
make replay
```

Para detener y limpiar todo:

```bash
docker compose down -v   # o: make down
```

## Pruebas y estilo

```bash
make test    # uv run pytest -q
make lint    # uv run ruff check src tests
```

## Estructura

```
.
├── docker-compose.yml          Kafka (KRaft) + creacion de topicos
├── Makefile                    atajos de operacion
├── pyproject.toml              dependencias (uv)
├── src/
│   ├── transforms.py           contrato, validacion, ventanas, dedup, agregacion
│   ├── pipeline.py             pipeline Beam (Kafka o archivo)
│   ├── producer.py             productor sintetico / replay
│   └── consumer.py             consumidor con materializacion upsert
├── tests/                      pruebas: contrato, ventanas, duplicados, tardios
├── data/eventos-ejemplo.jsonl  dataset de ejemplo con casos adversos
├── scripts/smoke_test.sh       smoke test end-to-end en modo archivo
├── docs/                       documento tecnico, diagrama, guion de demo
└── evidencia/                  salida de pruebas y ejecucion
```

## Parametros del pipeline

| Parametro | Default | Descripcion |
|---|---|---|
| --source | file | fuente: kafka o file |
| --sink | file | salida: kafka o file |
| --window-seconds | 60 | tamano de la ventana fija |
| --allowed-lateness | 120 | tolerancia de datos tardios (segundos) |
| --early-seconds | None | pane temprano cada N s de processing time (solo streaming) |

## Semantica de entrega

At-least-once de Kafka a Beam, convertida en un efecto observable idempotente por
la clave de salida `tienda_id|window_start` y la deduplicacion por `event_id`. No
se afirma exactly-once end-to-end; el detalle esta en el documento tecnico,
seccion 6.
