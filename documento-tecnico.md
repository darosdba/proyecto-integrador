# Documento tecnico - Pipeline de ventas de retail en streaming

Proyecto integrador - Streaming de datos y sus aplicaciones
Maestria en Inteligencia Artificial - FPUNA

## 1. Problema, usuarios y decisiones que habilita

Una cadena de retail vende de forma continua en varias tiendas fisicas. Hoy la
informacion de ventas se consolida por lote nocturno, lo que impide reaccionar
durante el dia. El proyecto construye un pipeline de streaming que calcula, en
tiempo casi real, el total vendido confirmado por tienda y por ventana de un
minuto.

Usuarios del resultado:

- Operaciones y jefes de tienda: ven la venta del intervalo en curso para
  reaccionar (quiebres, ritmo de ventas, promociones).
- Reposicion y logistica: usan el ritmo por tienda para priorizar.
- Analitica: consume la serie por tienda y ventana para comparar periodos.

El resultado habilita decisiones operativas dentro del dia en lugar de esperar
al cierre nocturno, y lo hace tolerando la realidad de un sistema distribuido:
eventos que llegan duplicados, fuera de orden o tarde.

## 2. Arquitectura

El flujo de datos sigue cuatro etapas (ver `docs/diagrama-arquitectura.png`):

1. Fuente (`src/producer.py`): productor sintetico reproducible o replay de un
   archivo JSONL. Publica en Kafka usando `tienda_id` como clave. Inyecta a
   proposito duplicados, eventos tardios/desordenados y algunos invalidos.
2. Kafka (`docker-compose.yml`, modo KRaft): topico de entrada `ventas.raw`,
   topico derivado `ventas.totales` (compactado, para upsert) y `ventas.invalidos`
   (dead-letter).
3. Apache Beam (`src/pipeline.py`, `src/transforms.py`, DirectRunner): lee con
   KafkaIO, valida el contrato, ancla el tiempo de evento, ventanea, deduplica
   con estado, agrega con un combinador incremental y publica los totales.
4. Salida (`src/consumer.py`): consume `ventas.totales` y materializa una fila
   por ventana con upsert (conserva el ultimo pane por clave idempotente).

### Componentes de Beam

- `ParsearYValidar` (ParDo con salidas laterales): separa validos de invalidos;
  los invalidos van al dead-letter con la razon del rechazo.
- `asignar_timestamp`: ancla cada evento a su `event_time` de dominio.
- `TotalesPorTienda` (PTransform compuesto): filtra confirmadas, aplica la
  ventana fija con la politica de triggers, deduplica, extrae el monto, agrega
  con `SumaMontos` (CombineFn) y formatea la salida idempotente.
- `Deduplicar` (DoFn con estado y timer): descarta `event_id` repetidos por
  comercio y ventana; un timer de event time libera el estado.

## 3. Contrato de eventos

Cada evento de `ventas.raw` es un JSON con:

| Campo | Rol | Ejemplo |
|---|---|---|
| event_id | identidad unica y estable, base de la deduplicacion | evt-9f2a1c |
| schema_version | version del contrato para evolucion | "1.0" |
| tienda_id | clave de negocio y de particionamiento | "T-014" |
| caja_id | caja de origen (payload) | "T-014-C03" |
| event_time | momento del hecho en el dominio (ISO-8601 UTC) | "2026-07-24T13:00:10Z" |
| monto | importe de la venta (payload) | 120000 |
| medio_pago | payload | "tarjeta_credito" |
| estado | CONFIRMADA / PENDIENTE / ANULADA | "CONFIRMADA" |

Solo se suman las ventas `CONFIRMADA`. `event_time` es el tiempo de dominio y se
mantiene separado del tiempo de procesamiento.

Versionado / evolucion: el campo `schema_version` viaja en cada evento. La
validacion acepta la version soportada ("1.0") y desvia al dead-letter las
versiones desconocidas. La estrategia de evolucion es aditiva: se agregan campos
opcionales sin romper consumidores; un cambio incompatible sube la version mayor
y se maneja como contrato nuevo.

## 4. Kafka: topicos, claves y particiones

| Topico | Particiones | Clave | Politica | Proposito |
|---|---|---|---|---|
| ventas.raw | 6 | tienda_id | retencion 7 dias | eventos crudos de venta |
| ventas.totales | 6 | tienda_id\|window_start | compactado (upsert) | totales por ventana |
| ventas.invalidos | 3 | (sin clave) | retencion 14 dias | eventos que violan el contrato |

Clave `tienda_id` en la entrada: Kafka preserva orden dentro de una particion,
no entre particiones. Agrupar por tienda envia todos los eventos de una tienda a
la misma particion, lo que preserva el orden por comercio y permite mantener
estado por tienda (deduplicacion, acumulados). El total por ventana no depende
del orden exacto, pero la correlacion entre una venta y su posible anulacion si.

Particiones: 6 alcanza para el volumen del caso y deja margen para paralelizar el
consumo hasta 6 procesos por grupo. Aumentar particiones luego reescribiria el
hash y romperia el orden historico por clave, por eso se dimensiona desde el
inicio. Riesgo de hot partition: tiendas muy grandes concentran trafico; a esta
escala es tolerable y, de crecer, se pasaria a una clave compuesta.

`ventas.totales` se declara compactado porque su clave es logica
(`tienda_id|window_start`) y cada pane nuevo debe reemplazar al anterior: la
compactacion conserva el ultimo valor por clave, que es exactamente la semantica
de materializacion que queremos.

Esquema de salida (una fila por pane de ventana):

```json
{
  "clave_idempotente": "T-014|2026-07-24T13:00:00+00:00",
  "tienda_id": "T-014",
  "window_start": "2026-07-24T13:00:00+00:00",
  "window_end": "2026-07-24T13:01:00+00:00",
  "total": 250000,
  "cantidad": 3,
  "pane": "ON_TIME"
}
```

## 5. Tiempo de evento, ventanas y datos tardios

- Tiempo de evento: se asigna el timestamp del pipeline a partir del `event_time`
  del dominio, no del momento de llegada. Una venta de las 13:00:50 que llega a
  las 13:02 pertenece a la ventana [13:00, 13:01).
- Ventana: fija de 60 segundos, intervalos semiabiertos [inicio, fin). Es el
  detalle operativo del tablero: corto para reaccionar, comparable entre dias.
- Watermark: heuristico, provisto por KafkaIO segun el avance de las particiones.
- Allowed lateness: 120 segundos. La ventana acepta correcciones hasta que el
  watermark supera fin + 120 s; despues el estado se libera y lo que llega se
  descarta.
- Triggers (modo streaming): `AfterWatermark` con pane temprano cada 10 s de
  processing time (early), pane on-time al pasar el watermark, y pane late por
  cada evento tardio aceptado. Modo ACCUMULATING: cada pane trae el total
  acumulado, que reemplaza al anterior via upsert.
- Modo archivo (batch, para pruebas y evidencia): se emite un unico pane por
  ventana (sin early/late), porque el batch dispone de todos los datos.

## 6. Duplicados, idempotencia y semantica de entrega

- Deduplicacion: `Deduplicar` mantiene un `SetState` de `event_id` por clave de
  comercio y ventana. La primera aparicion se emite; las repeticiones se
  descartan. Un timer de event time limpia el estado en fin de ventana + lateness;
  sin esa expiracion el estado creceria indefinidamente.
- Horizonte de deduplicacion: coincide con la vida de la ventana mas la lateness
  (60 s + 120 s). Un duplicado que reaparezca despues de ese horizonte ya no se
  detecta; es una decision explicita para acotar el estado.
- Idempotencia de salida: la clave `tienda_id|window_start` permite que el
  consumidor haga upsert. Reprocesar o reintentar publica la misma clave con el
  total acumulado, y el estado final del sink no cambia.
- Semantica de entrega declarada: at-least-once de Kafka a Beam (el consumidor de
  Beam puede reprocesar tras un fallo), convertida en un efecto observable
  idempotente por la clave de salida y la deduplicacion por event_id. No se
  afirma exactly-once end-to-end: la escritura a Kafka con KafkaIO en DirectRunner
  no coordina un commit atomico entre el offset de entrada y la salida; la
  correccion se sostiene por idempotencia, no por una transaccion global.

## 7. Pruebas y evidencia

- Pruebas unitarias (`tests/`): contrato y validacion, combinador de agregacion,
  separacion de invalidos, ventaneo por tiempo de evento con TestStream,
  duplicado que no altera el total, aislamiento de estado entre comercios, y
  evento tardio que produce revision mas evento demasiado tardio descartado.
- Evidencia end-to-end en modo archivo (`evidencia/evidencia-modo-archivo.md`):
  ejecuta el pipeline sobre `data/eventos-ejemplo.jsonl` y verifica los totales,
  la deduplicacion, el filtrado por estado y el dead-letter.
- Demostracion Kafka (guion en `docs/guion-demo.md`): productor -> ventas.raw ->
  pipeline -> ventas.totales -> consumidor, mostrando eventos normales,
  duplicados y tardios en vivo.

## 8. Limites, supuestos y mejoras

Supuestos:

- Un solo broker Kafka y DirectRunner: entorno de demostracion, no de produccion.
- El reloj de las cajas es razonablemente confiable; los relojes muy desviados se
  desvian por validacion.
- El volumen del caso entra con holgura en 6 particiones.

Limites conocidos:

- No hay exactly-once end-to-end (ver seccion 6).
- El watermark es heuristico: un evento fuera de la lateness se descarta.
- KafkaIO en Python es un transform cross-language: la primera ejecucion descarga
  imagenes de Beam Java y requiere Docker disponible en el host que corre el
  pipeline.
- La deduplicacion es por ventana + lateness; no cubre duplicados historicos.

Mejoras posibles:

- Ejecutar sobre Flink para tolerancia a fallos y checkpoints reales.
- Registro de esquemas (Schema Registry) para el versionado.
- Metricas y dashboard de lag, panes por ventana y tasa de tardios.
- Inferencia en linea (por ejemplo, deteccion de anomalias de venta por caja).
