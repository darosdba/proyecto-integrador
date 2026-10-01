"""Ventanas por tiempo de evento: el orden de llegada no cambia la ventana."""

from __future__ import annotations

from datetime import UTC, datetime

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

import transforms as t

BASE = datetime(2026, 7, 24, 13, 0, 0, tzinfo=UTC).timestamp()
W0 = "2026-07-24T13:00:00+00:00"
W1 = "2026-07-24T13:01:00+00:00"


def ev(event_id, tienda, offset, monto, estado="CONFIRMADA"):
    return {
        "event_id": event_id,
        "schema_version": "1.0",
        "tienda_id": tienda,
        "event_time": "n/a",
        "monto": monto,
        "estado": estado,
    }


def test_ventana_usa_tiempo_de_evento_no_de_llegada():
    stream = (
        TestStream()
        # e2 (13:00:50) se agrega antes que e_ooo (13:00:30): llega desordenado
        .add_elements([TimestampedValue(ev("e1", "T-014", 10, 100), BASE + 10)])
        .add_elements([TimestampedValue(ev("e2", "T-014", 50, 200), BASE + 50)])
        .add_elements([TimestampedValue(ev("eooo", "T-002", 30, 500), BASE + 30)])
        # evento no confirmado: debe filtrarse
        .add_elements(
            [TimestampedValue(ev("ep", "T-014", 40, 999, "PENDIENTE"), BASE + 40)]
        )
        .add_elements([TimestampedValue(ev("e4", "T-014", 80, 300), BASE + 80)])
        .advance_watermark_to_infinity()
    )

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with TestPipeline(options=options) as p:
        salida = (
            p
            | stream
            | t.TotalesPorTienda(window_seconds=60, allowed_lateness_seconds=120)
            | "mapear"
            >> __import__("apache_beam").Map(
                lambda r: (r["tienda_id"], r["window_start"], r["total"], r["cantidad"])
            )
        )
        assert_that(
            salida,
            equal_to(
                [
                    ("T-014", W0, 300, 2),  # e1 + e2 (misma ventana por event time)
                    ("T-002", W0, 500, 1),
                    ("T-014", W1, 300, 1),  # e4 en la ventana siguiente
                ]
            ),
        )
