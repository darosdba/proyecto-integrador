"""Pipeline end-to-end de totales de ventas por comercio y ventana.

Soporta dos fuentes y dos salidas:

- fuente `kafka`  : lee el topico de entrada con KafkaIO (streaming).
- fuente `file`   : reproduce un archivo JSONL (util para smoke test sin Kafka).
- salida `kafka`  : publica los totales con KafkaIO en el topico derivado.
- salida `file`   : escribe los totales como JSONL (evidencia reproducible).

Ejemplos:
    # Modo archivo (no requiere Kafka), genera evidencia:
    python src/pipeline.py --source file --sink file \
        --input data/eventos-ejemplo.jsonl --output out/totales

    # Modo Kafka end-to-end (requiere docker compose up):
    python src/pipeline.py --source kafka --sink kafka \
        --bootstrap localhost:9092 \
        --input-topic ventas.raw --output-topic ventas.totales
"""

from __future__ import annotations

import argparse
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

import transforms as t


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Pipeline de totales de ventas")
    parser.add_argument("--source", choices=["kafka", "file"], default="file")
    parser.add_argument("--sink", choices=["kafka", "file"], default="file")
    parser.add_argument("--input", default="data/eventos-ejemplo.jsonl")
    parser.add_argument("--output", default="out/totales")
    parser.add_argument("--invalid-output", default="out/invalidos")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--input-topic", default="ventas.raw")
    parser.add_argument("--output-topic", default="ventas.totales")
    parser.add_argument("--invalid-topic", default="ventas.invalidos")
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--allowed-lateness", type=int, default=120)
    parser.add_argument(
        "--early-seconds",
        type=int,
        default=None,
        help="pane temprano cada N segundos de processing time (solo streaming)",
    )
    return parser.parse_known_args(argv)


def _leer_crudos(pipeline, args):
    if args.source == "kafka":
        from apache_beam.io.kafka import ReadFromKafka

        return (
            pipeline
            | "LeerKafka"
            >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": args.bootstrap,
                    "auto.offset.reset": "earliest",
                    "group.id": "ventas-beam",
                },
                topics=[args.input_topic],
            )
            | "SoloValor" >> beam.Map(lambda kv: kv[1])
        )
    return pipeline | "LeerArchivo" >> beam.io.ReadFromText(args.input)


def _escribir_totales(totales, args):
    if args.sink == "kafka":
        from apache_beam.io.kafka import WriteToKafka

        return (
            totales
            | "ClaveValorKafka"
            >> beam.Map(
                lambda r: (
                    r["clave_idempotente"].encode("utf-8"),
                    json.dumps(r, ensure_ascii=False).encode("utf-8"),
                )
            )
            | "EscribirKafka"
            >> WriteToKafka(
                producer_config={"bootstrap.servers": args.bootstrap},
                topic=args.output_topic,
            )
        )
    return (
        totales
        | "TotalesAJson" >> beam.Map(lambda r: json.dumps(r, ensure_ascii=False))
        | "EscribirTotales"
        >> beam.io.WriteToText(args.output, file_name_suffix=".jsonl")
    )


def _escribir_invalidos(invalidos, args):
    if args.sink == "kafka":
        from apache_beam.io.kafka import WriteToKafka

        return (
            invalidos
            | "InvalidosKafkaKV"
            >> beam.Map(
                lambda r: (b"invalido", json.dumps(r, ensure_ascii=False).encode("utf-8"))
            )
            | "EscribirInvalidosKafka"
            >> WriteToKafka(
                producer_config={"bootstrap.servers": args.bootstrap},
                topic=args.invalid_topic,
            )
        )
    return (
        invalidos
        | "InvalidosAJson" >> beam.Map(lambda r: json.dumps(r, ensure_ascii=False))
        | "EscribirInvalidos"
        >> beam.io.WriteToText(args.invalid_output, file_name_suffix=".jsonl")
    )


def construir_pipeline(pipeline, args):
    clasificados = (
        _leer_crudos(pipeline, args)
        | "ParsearValidar"
        >> beam.ParDo(t.ParsearYValidar()).with_outputs(
            t.TAG_VALIDOS, t.TAG_INVALIDOS
        )
    )

    # En modo archivo (batch) se emite un unico pane por ventana; en Kafka
    # (streaming) se habilitan las revisiones tardias.
    es_streaming = args.source == "kafka" or args.sink == "kafka"
    totales = (
        clasificados[t.TAG_VALIDOS]
        | "AnclarTiempoEvento" >> beam.Map(t.asignar_timestamp)
        | "Totales"
        >> t.TotalesPorTienda(
            window_seconds=args.window_seconds,
            allowed_lateness_seconds=args.allowed_lateness,
            early_seconds=args.early_seconds if es_streaming else None,
            late_firing=es_streaming,
        )
    )

    _escribir_totales(totales, args)
    _escribir_invalidos(clasificados[t.TAG_INVALIDOS], args)
    return totales


def main(argv=None):
    args, beam_argv = _parse_args(argv)
    es_streaming = args.source == "kafka" or args.sink == "kafka"
    if not es_streaming:
        # batch: el unico pane por ventana usa AfterWatermark sin late; el flag
        # habilita ese trigger, seguro aqui porque el batch tiene todos los datos.
        beam_argv = [*beam_argv, "--allow_unsafe_triggers"]
    options = PipelineOptions(beam_argv)
    if es_streaming:
        options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        construir_pipeline(pipeline, args)


if __name__ == "__main__":
    main()
