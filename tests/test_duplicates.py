"""Un event_id duplicado dentro del comercio no modifica el total."""

from __future__ import annotations

from datetime import UTC, datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

import transforms as t

BASE = datetime(2026, 7, 24, 13, 0, 0, tzinfo=UTC).timestamp()
W0 = "2026-07-24T13:00:00+00:00"


def ev(event_id, tienda, monto):
    return {
        "event_id": event_id,
        "schema_version": "1.0",
        "tienda_id": tienda,
        "event_time": "n/a",
        "monto": monto,
        "estado": "CONFIRMADA",
    }


def test_duplicado_no_cambia_el_total():
    stream = (
        TestStream()
        .add_elements([TimestampedValue(ev("p-1", "T-014", 100), BASE + 10)])
        # mismo event_id p-1: duplicado, debe descartarse
        .add_elements([TimestampedValue(ev("p-1", "T-014", 100), BASE + 20)])
        .add_elements([TimestampedValue(ev("p-2", "T-014", 50), BASE + 30)])
        .advance_watermark_to_infinity()
    )
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with TestPipeline(options=options) as p:
        salida = (
            p
            | stream
            | t.TotalesPorTienda(window_seconds=60, allowed_lateness_seconds=120)
            | beam.Map(lambda r: (r["tienda_id"], r["window_start"], r["total"], r["cantidad"]))
        )
        # total 150 (no 250) y cantidad 2 (no 3)
        assert_that(salida, equal_to([("T-014", W0, 150, 2)]))


def test_mismo_event_id_en_otro_comercio_no_colisiona():
    stream = (
        TestStream()
        .add_elements([TimestampedValue(ev("compartido", "T-a", 10), BASE + 10)])
        .add_elements([TimestampedValue(ev("compartido", "T-b", 20), BASE + 15)])
        .advance_watermark_to_infinity()
    )
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with TestPipeline(options=options) as p:
        salida = (
            p
            | stream
            | t.TotalesPorTienda(window_seconds=60, allowed_lateness_seconds=120)
            | beam.Map(lambda r: (r["tienda_id"], r["total"]))
        )
        assert_that(salida, equal_to([("T-a", 10), ("T-b", 20)]))
