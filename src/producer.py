"""Productor de eventos de venta hacia Kafka.

Dos modos:

- synthetic: genera eventos continuos y reproducibles (semilla fija), e inyecta
  a proposito duplicados, eventos fuera de orden / tardios y algunos invalidos
  para ejercitar la validacion, la deduplicacion y la politica temporal.
- replay: reproduce un archivo JSONL conservando el orden, con control de
  velocidad.

La clave del mensaje Kafka es tienda_id (particionamiento y orden por comercio).

Ejemplos:
    python src/producer.py --mode synthetic --bootstrap localhost:9092 \
        --topic ventas.raw --rate 5 --count 200
    python src/producer.py --mode replay --input data/eventos-ejemplo.jsonl \
        --topic ventas.raw
"""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import UTC, datetime, timedelta

from kafka import KafkaProducer

TIENDAS = ["T-001", "T-002", "T-014", "T-027", "T-033"]
MEDIOS = ["efectivo", "tarjeta_credito", "tarjeta_debito", "qr"]
SCHEMA_VERSION = "1.0"


def crear_productor(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=20,
    )


def nuevo_evento(rng: random.Random, ahora: datetime) -> dict:
    tienda = rng.choice(TIENDAS)
    return {
        "event_id": "evt-" + uuid.UUID(int=rng.getrandbits(128)).hex[:16],
        "schema_version": SCHEMA_VERSION,
        "tienda_id": tienda,
        "caja_id": f"{tienda}-C{rng.randint(1, 4):02d}",
        "event_time": ahora.isoformat().replace("+00:00", "Z"),
        "monto": rng.choice([15000, 25000, 50000, 89000, 120000, 200000, 429000]),
        "medio_pago": rng.choice(MEDIOS),
        "estado": rng.choices(
            ["CONFIRMADA", "PENDIENTE", "ANULADA"], weights=[85, 8, 7]
        )[0],
    }


def enviar(productor, topic, evento, contador):
    productor.send(topic, key=evento["tienda_id"], value=evento)
    contador[evento["estado"]] = contador.get(evento["estado"], 0) + 1


def modo_sintetico(args):
    rng = random.Random(args.seed)
    productor = crear_productor(args.bootstrap)
    contador: dict[str, int] = {}
    ultimo = None
    n_dup = n_late = n_inval = 0

    print(f"Produciendo {args.count} eventos en '{args.topic}' a {args.rate}/s...")
    for i in range(args.count):
        ahora = datetime.now(UTC)

        # duplicado: reenviar el evento anterior con el mismo event_id
        if ultimo is not None and rng.random() < args.duplicate_rate:
            enviar(productor, args.topic, ultimo, contador)
            n_dup += 1
            print(f"  [{i}] DUPLICADO event_id={ultimo['event_id']}")
            time.sleep(1.0 / args.rate)
            continue

        evento = nuevo_evento(rng, ahora)

        # tardio / fuera de orden: retroceder el event_time
        if rng.random() < args.late_rate:
            atraso = rng.randint(90, 200)
            evento["event_time"] = (
                (ahora - timedelta(seconds=atraso)).isoformat().replace("+00:00", "Z")
            )
            n_late += 1
            print(f"  [{i}] TARDIO {atraso}s tienda={evento['tienda_id']}")

        # invalido: romper el contrato para ejercitar el dead-letter
        if rng.random() < args.invalid_rate:
            evento.pop("monto", None)
            n_inval += 1
            print(f"  [{i}] INVALIDO (sin monto) tienda={evento['tienda_id']}")

        enviar(productor, args.topic, evento, contador)
        ultimo = evento
        time.sleep(1.0 / args.rate)

    productor.flush()
    productor.close()
    print("\nResumen de produccion:")
    print(f"  por estado: {contador}")
    print(f"  duplicados: {n_dup}  tardios: {n_late}  invalidos: {n_inval}")


def modo_replay(args):
    productor = crear_productor(args.bootstrap)
    enviados = 0
    with open(args.input, encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            evento = json.loads(linea)
            clave = evento.get("tienda_id", "sin-clave")
            productor.send(args.topic, key=clave, value=evento)
            enviados += 1
            time.sleep(args.replay_delay)
    productor.flush()
    productor.close()
    print(f"Replay completo: {enviados} eventos enviados a '{args.topic}'.")


def main():
    parser = argparse.ArgumentParser(description="Productor de eventos de venta")
    parser.add_argument("--mode", choices=["synthetic", "replay"], default="synthetic")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="ventas.raw")
    parser.add_argument("--rate", type=float, default=5.0, help="eventos por segundo")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--duplicate-rate", type=float, default=0.10)
    parser.add_argument("--late-rate", type=float, default=0.12)
    parser.add_argument("--invalid-rate", type=float, default=0.05)
    parser.add_argument("--input", default="data/eventos-ejemplo.jsonl")
    parser.add_argument("--replay-delay", type=float, default=0.2)
    args = parser.parse_args()

    if args.mode == "synthetic":
        modo_sintetico(args)
    else:
        modo_replay(args)


if __name__ == "__main__":
    main()
