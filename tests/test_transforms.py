"""Pruebas unitarias de la logica de contrato, validacion y agregacion."""

from __future__ import annotations

from datetime import UTC, datetime

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

import transforms as t


def _evento(**kwargs):
    base = {
        "event_id": "evt-1",
        "schema_version": "1.0",
        "tienda_id": "T-014",
        "event_time": "2026-07-24T13:00:05Z",
        "monto": 100,
        "estado": "CONFIRMADA",
    }
    base.update(kwargs)
    return base


def test_parse_utc_es_timezone_aware():
    parsed = t.parse_utc("2026-07-24T13:00:05Z")
    assert parsed == datetime(2026, 7, 24, 13, 0, 5, tzinfo=UTC)
    assert parsed.utcoffset() is not None


def test_validar_evento_correcto_no_tiene_errores():
    assert t.validar_evento(_evento()) == []


def test_validar_evento_detecta_campos_y_valores_invalidos():
    errores = t.validar_evento(
        _evento(estado="XXX", monto=-10, event_time="no-fecha")
    )
    assert any("estado" in e for e in errores)
    assert any("monto" in e for e in errores)
    assert any("event_time" in e for e in errores)


def test_validar_evento_detecta_campo_faltante():
    ev = _evento()
    del ev["tienda_id"]
    errores = t.validar_evento(ev)
    assert any("tienda_id" in e for e in errores)


def test_combinefn_suma_total_y_cantidad():
    combine = t.SumaMontos()
    acc = combine.create_accumulator()
    for monto in (100, 200, 50):
        acc = combine.add_input(acc, monto)
    assert combine.extract_output(acc) == {"total": 350, "cantidad": 3}


def test_parsear_y_validar_separa_validos_de_invalidos():
    entradas = [
        '{"event_id":"e1","schema_version":"1.0","tienda_id":"T-1",'
        '"event_time":"2026-07-24T13:00:00Z","monto":100,"estado":"CONFIRMADA"}',
        '{"event_id":"e2","schema_version":"1.0","tienda_id":"T-1",'
        '"event_time":"2026-07-24T13:00:00Z","monto":-1,"estado":"CONFIRMADA"}',
        "esto no es json",
    ]
    with TestPipeline() as p:
        salidas = (
            p
            | beam.Create(entradas)
            | beam.ParDo(t.ParsearYValidar()).with_outputs(
                t.TAG_VALIDOS, t.TAG_INVALIDOS
            )
        )
        validos = salidas[t.TAG_VALIDOS] | "idv" >> beam.Map(
            lambda ev: ev["event_id"]
        )
        cant_invalidos = (
            salidas[t.TAG_INVALIDOS]
            | "contar" >> beam.combiners.Count.Globally()
        )
        assert_that(validos, equal_to(["e1"]), label="validos")
        assert_that(cant_invalidos, equal_to([2]), label="invalidos")
