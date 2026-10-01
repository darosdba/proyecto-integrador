"""Nucleo del pipeline de ventas: contrato de evento y transformaciones Beam.

Este modulo no depende de Kafka. Contiene la logica pura de validacion,
ventaneo por tiempo de evento, deduplicacion con estado y agregacion por
comercio, de modo que pueda probarse con TestPipeline y TestStream.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms import trigger
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer
from apache_beam.utils.timestamp import Duration, Timestamp
from apache_beam.utils.windowed_value import PaneInfoTiming

# --- Contrato de evento -----------------------------------------------------

SCHEMA_VERSION = "1.0"
ESTADOS_VALIDOS = {"CONFIRMADA", "ANULADA", "PENDIENTE"}
ESTADO_QUE_SUMA = "CONFIRMADA"
CAMPOS_REQUERIDOS = (
    "event_id",
    "schema_version",
    "tienda_id",
    "event_time",
    "monto",
    "estado",
)

# Salidas con nombre (tags) para separar validos de invalidos.
TAG_VALIDOS = "validos"
TAG_INVALIDOS = "invalidos"


def parse_utc(raw_value: str) -> datetime:
    """Convertir un timestamp ISO-8601 (con Z o offset) a datetime UTC aware."""
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ValueError(f"event_time vacio o no textual: {raw_value!r}")
    text = raw_value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_event(raw: Any) -> dict[str, Any]:
    """Aceptar un evento como dict, str JSON o bytes JSON y devolver dict."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


def validar_evento(evento: dict[str, Any]) -> list[str]:
    """Devolver la lista de errores del contrato; vacia si el evento es valido."""
    errores: list[str] = []
    for campo in CAMPOS_REQUERIDOS:
        if campo not in evento or evento[campo] in (None, ""):
            errores.append(f"falta campo requerido: {campo}")
    if errores:
        return errores

    if evento["schema_version"] != SCHEMA_VERSION:
        errores.append(f"schema_version no soportada: {evento['schema_version']}")
    if evento["estado"] not in ESTADOS_VALIDOS:
        errores.append(f"estado invalido: {evento['estado']}")
    try:
        parse_utc(evento["event_time"])
    except (ValueError, TypeError):
        errores.append(f"event_time invalido: {evento.get('event_time')!r}")
    monto = evento.get("monto")
    if not isinstance(monto, (int, float)) or isinstance(monto, bool) or monto < 0:
        errores.append(f"monto invalido: {monto!r}")
    return errores


class ParsearYValidar(beam.DoFn):
    """Parsear el valor crudo, validarlo y separarlo en validos / invalidos."""

    def process(self, raw: Any):
        try:
            evento = parse_event(raw)
        except (ValueError, TypeError) as exc:
            yield beam.pvalue.TaggedOutput(
                TAG_INVALIDOS,
                {"raw": str(raw)[:500], "errores": [f"json invalido: {exc}"]},
            )
            return

        errores = validar_evento(evento)
        if errores:
            yield beam.pvalue.TaggedOutput(
                TAG_INVALIDOS, {"evento": evento, "errores": errores}
            )
        else:
            yield beam.pvalue.TaggedOutput(TAG_VALIDOS, evento)


def asignar_timestamp(evento: dict[str, Any]) -> beam.window.TimestampedValue:
    """Anclar el evento a su tiempo de evento del dominio."""
    epoch = parse_utc(evento["event_time"]).timestamp()
    return beam.window.TimestampedValue(evento, epoch)


def clave_por_tienda(evento: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    return evento["tienda_id"], evento


class SumaMontos(beam.CombineFn):
    """Combinador incremental: total vendido y cantidad de ventas por clave."""

    def create_accumulator(self) -> tuple[int, int]:
        return (0, 0)

    def add_input(self, acc: tuple[int, int], monto: int) -> tuple[int, int]:
        total, cantidad = acc
        return (total + monto, cantidad + 1)

    def merge_accumulators(self, accs) -> tuple[int, int]:
        totales, cantidades = zip(*accs, strict=False)
        return (sum(totales), sum(cantidades))

    def extract_output(self, acc: tuple[int, int]) -> dict[str, int]:
        total, cantidad = acc
        return {"total": total, "cantidad": cantidad}


class Deduplicar(beam.DoFn):
    """Descartar event_id repetidos dentro de cada comercio y ventana.

    Mantiene un SetState de event_id vistos por clave, y un timer de event
    time que limpia el estado cuando el watermark supera el fin de la ventana
    mas la lateness permitida. Sin esa expiracion el estado creceria sin fin.
    """

    IDS_VISTOS = SetStateSpec("ids_vistos", StrUtf8Coder())
    EXPIRACION = TimerSpec("expiracion", TimeDomain.WATERMARK)

    def __init__(self, allowed_lateness_seconds: int = 120):
        self.allowed_lateness_seconds = allowed_lateness_seconds

    def process(
        self,
        element: tuple[str, dict[str, Any]],
        ids_vistos=beam.DoFn.StateParam(IDS_VISTOS),
        window=beam.DoFn.WindowParam,
        expiracion=beam.DoFn.TimerParam(EXPIRACION),
    ):
        _clave, evento = element
        event_id = evento["event_id"]
        if event_id in set(ids_vistos.read()):
            return
        ids_vistos.add(event_id)
        expiracion.set(window.end + Duration(self.allowed_lateness_seconds))
        yield element

    @on_timer(EXPIRACION)
    def expirar(self, ids_vistos=beam.DoFn.StateParam(IDS_VISTOS)):
        ids_vistos.clear()


class FormatearTotal(beam.DoFn):
    """Convertir (tienda, {total, cantidad}) en la fila de salida idempotente."""

    def process(self, element, window=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        tienda_id, agregado = element
        inicio = window.start.to_utc_datetime().replace(tzinfo=UTC)
        fin = window.end.to_utc_datetime().replace(tzinfo=UTC)
        clave_idempotente = f"{tienda_id}|{inicio.isoformat()}"
        yield {
            "clave_idempotente": clave_idempotente,
            "tienda_id": tienda_id,
            "window_start": inicio.isoformat(),
            "window_end": fin.isoformat(),
            "total": agregado["total"],
            "cantidad": agregado["cantidad"],
            "pane": _nombre_pane(pane),
        }


def _nombre_pane(pane) -> str:
    if pane is None:
        return "UNKNOWN"
    if pane.timing == PaneInfoTiming.EARLY:
        return "EARLY"
    if pane.timing == PaneInfoTiming.ON_TIME:
        return "ON_TIME"
    if pane.timing == PaneInfoTiming.LATE:
        return "LATE"
    return "UNKNOWN"


def construir_trigger(
    *,
    allowed_lateness_seconds: int,
    early_seconds: int | None,
    late_firing: bool = True,
) -> trigger.TriggerFn:
    """Trigger AfterWatermark: on-time por watermark, early opcional, late por evento.

    En modo batch (archivo) conviene late_firing=False para emitir un unico pane
    por ventana; en streaming se deja en True para materializar revisiones.
    """
    early = (
        trigger.Repeatedly(trigger.AfterProcessingTime(early_seconds))
        if early_seconds is not None
        else None
    )
    late = trigger.Repeatedly(trigger.AfterCount(1)) if late_firing else None
    return trigger.AfterWatermark(early=early, late=late)


class TotalesPorTienda(beam.PTransform):
    """Totales confirmados por comercio y ventana, tolerando duplicados y tardios.

    Espera una PCollection de eventos validos ya anclados a su tiempo de evento
    (via TimestampedValue). Devuelve filas de total por (tienda, ventana).
    """

    def __init__(
        self,
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        early_seconds: int | None = None,
        late_firing: bool = True,
    ):
        super().__init__()
        self.window_seconds = window_seconds
        self.allowed_lateness_seconds = allowed_lateness_seconds
        self.early_seconds = early_seconds
        self.late_firing = late_firing

    def expand(self, eventos):
        return (
            eventos
            | "SoloConfirmadas"
            >> beam.Filter(lambda ev: ev.get("estado") == ESTADO_QUE_SUMA)
            | "VentanaPorTiempoEvento"
            >> beam.WindowInto(
                beam.window.FixedWindows(self.window_seconds),
                trigger=construir_trigger(
                    allowed_lateness_seconds=self.allowed_lateness_seconds,
                    early_seconds=self.early_seconds,
                    late_firing=self.late_firing,
                ),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=self.allowed_lateness_seconds,
            )
            | "ClavePorTienda" >> beam.Map(clave_por_tienda)
            | "Deduplicar"
            >> beam.ParDo(Deduplicar(self.allowed_lateness_seconds))
            | "ExtraerMonto"
            >> beam.Map(lambda kv: (kv[0], kv[1]["monto"]))
            | "SumarPorTienda" >> beam.CombinePerKey(SumaMontos())
            | "Formatear" >> beam.ParDo(FormatearTotal())
        )


def epoch_de(evento: dict[str, Any]) -> Timestamp:
    """Helper para tests: Timestamp Beam a partir del event_time del evento."""
    return Timestamp(parse_utc(evento["event_time"]).timestamp())
