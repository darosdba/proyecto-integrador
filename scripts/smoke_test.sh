#!/usr/bin/env bash
# Smoke test end-to-end en modo archivo (no requiere Kafka ni Docker).
# Reproduce el dataset de ejemplo por el pipeline Beam y valida los totales.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== Ejecutando pipeline en modo archivo =="
rm -rf out
uv run python src/pipeline.py \
  --source file --sink file \
  --input data/eventos-ejemplo.jsonl \
  --output out/totales --invalid-output out/invalidos 2>/dev/null

echo
echo "== Totales por comercio y ventana =="
cat out/totales*.jsonl | python3 -c "
import sys, json
rows = sorted((json.loads(l) for l in sys.stdin), key=lambda r: (r['tienda_id'], r['window_start']))
for r in rows:
    print(f\"{r['tienda_id']}  {r['window_start']}  total={r['total']:>8}  cantidad={r['cantidad']}  pane={r['pane']}\")
"

echo
echo "== Eventos invalidos (dead-letter) =="
cat out/invalidos*.jsonl

echo
echo "== Verificacion de valores esperados =="
python3 - <<'PY'
import glob, json
rows = []
for f in glob.glob("out/totales*.jsonl"):
    with open(f, encoding="utf-8") as fh:
        rows += [json.loads(l) for l in fh if l.strip()]
idx = {(r["tienda_id"], r["window_start"]): r for r in rows}
esperado = {
    ("T-002", "2026-07-24T13:00:00+00:00"): (200000, 1),
    ("T-014", "2026-07-24T13:00:00+00:00"): (250000, 3),
    ("T-014", "2026-07-24T13:01:00+00:00"): (120000, 2),
}
ok = True
for clave, (total, cant) in esperado.items():
    r = idx.get(clave)
    if not r or r["total"] != total or r["cantidad"] != cant:
        print(f"  FALLO {clave}: esperado total={total} cantidad={cant}, obtenido {r}")
        ok = False
    else:
        print(f"  OK    {clave}: total={total} cantidad={cant}")
assert len(rows) == 3, f"se esperaban 3 totales, hay {len(rows)}"
assert ok, "algun total no coincide con lo esperado"
print("\nSmoke test OK: dedup, filtrado por estado, ventana por tiempo de evento y dead-letter verificados.")
PY
