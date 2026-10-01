"""Consumidor del topico de totales.

Mantiene en memoria el ultimo valor por clave idempotente
(merchant|window_start), demostrando la materializacion tipo upsert: los panes
sucesivos de una misma ventana reemplazan al anterior en lugar de sumarse.

Ejemplo:
    python src/consumer.py --bootstrap localhost:9092 --topic ventas.totales
"""

from __future__ import annotations

import argparse
import json

from kafka import KafkaConsumer


def main():
    parser = argparse.ArgumentParser(description="Consumidor de totales")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="ventas.totales")
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=0,
        help="cortar tras N ms sin mensajes (0 = infinito)",
    )
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        group_id="ventas-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=args.timeout_ms or float("inf"),
    )

    estado: dict[str, dict] = {}
    print(f"Escuchando '{args.topic}' (upsert por clave idempotente)...\n")
    try:
        for registro in consumer:
            fila = registro.value
            clave = fila.get("clave_idempotente", registro.key)
            estado[clave] = fila
            print(
                f"{fila.get('pane','?'):8} {fila['tienda_id']:8} "
                f"{fila['window_start']} .. {fila['window_end']}  "
                f"total={fila['total']:>10}  cantidad={fila['cantidad']}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print("\n=== Estado final materializado (una fila por ventana) ===")
    for clave in sorted(estado):
        fila = estado[clave]
        print(
            f"{fila['tienda_id']:8} {fila['window_start']}  "
            f"total={fila['total']:>10}  cantidad={fila['cantidad']}"
        )


if __name__ == "__main__":
    main()
