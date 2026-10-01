# Evidencia end-to-end en modo archivo (replay)

Generada con: uv run python src/pipeline.py --source file --sink file --input data/eventos-ejemplo.jsonl
Entorno: Apache Beam 2.74.0, DirectRunner, Python 3.11.

## Pruebas (pytest)
```
..........                                                               [100%]
10 passed in 4.21s
```

## Salida del smoke test end-to-end
```
== Ejecutando pipeline en modo archivo ==

== Totales por comercio y ventana ==
T-002  2026-07-24T13:00:00+00:00  total=  200000  cantidad=1  pane=ON_TIME
T-014  2026-07-24T13:00:00+00:00  total=  250000  cantidad=3  pane=ON_TIME
T-014  2026-07-24T13:01:00+00:00  total=  120000  cantidad=2  pane=ON_TIME

== Eventos invalidos (dead-letter) ==
{"evento": {"event_id": "evt-010", "schema_version": "1.0", "tienda_id": "T-014", "caja_id": "T-014-C04", "event_time": "2026-07-24T13:01:10Z", "estado": "CONFIRMADA"}, "errores": ["falta campo requerido: monto"]}

== Verificacion de valores esperados ==
  OK    ('T-002', '2026-07-24T13:00:00+00:00'): total=200000 cantidad=1
  OK    ('T-014', '2026-07-24T13:00:00+00:00'): total=250000 cantidad=3
  OK    ('T-014', '2026-07-24T13:01:00+00:00'): total=120000 cantidad=2

Smoke test OK: dedup, filtrado por estado, ventana por tiempo de evento y dead-letter verificados.
```
