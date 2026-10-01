"""Evento tardio dentro de la tolerancia genera una revision; el fuera de
tolerancia se descarta."""

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


def ev(event_id, monto):
    return {
        "event_id": event_id,
        "schema_version": "1.0",
        "tienda_id": "T-014",
        "event_time": "n/a",
        "monto": monto,
        "estado": "CONFIRMADA",
    }


def test_tardio_dentro_de_tolerancia_revisa_y_fuera_se_descarta():
    stream = (
        TestStream()
        .add_elements([TimestampedValue(ev("e1", 100), BASE + 10)])
        .add_elements([TimestampedValue(ev("e2", 200), BASE + 30)])
        # el watermark pasa el fin de la ventana [13:00,13:01): pane ON_TIME
        .advance_watermark_to(BASE + 61)
        # tardio dentro de la lateness (120s): pane LATE que corrige el total
        .add_elements([TimestampedValue(ev("e3", 50), BASE + 45)])
        # el watermark supera fin + lateness (13:01 + 120s = 13:03)
        .advance_watermark_to(BASE + 210)
        # demasiado tardio: se descarta, no aparece en la salida
        .add_elements([TimestampedValue(ev("e4", 999), BASE + 5)])
        .advance_watermark_to_infinity()
    )
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with TestPipeline(options=options) as p:
        salida = (
            p
            | stream
            | t.TotalesPorTienda(window_seconds=60, allowed_lateness_seconds=120)
            | beam.Map(lambda r: (r["window_start"], r["total"], r["cantidad"], r["pane"]))
        )
        assert_that(
            salida,
            equal_to(
                [
                    (W0, 300, 2, "ON_TIME"),  # e1 + e2
                    (W0, 350, 3, "LATE"),  # + e3 tardio aceptado (revision)
                ]
            ),
        )
