#!/bin/bash
# Deploy del proyecto Hansard CR a Databricks
# Uso: ./scripts/deploy.sh [dev|prod]

set -e

TARGET="${1:-dev}"

echo "→ Validando bundle..."
databricks bundle validate --target "$TARGET"

echo "→ Desplegando notebooks, job y app..."
databricks bundle deploy --target "$TARGET"

echo "→ Estado del App:"
databricks apps get hansard-cr-app || true

echo ""
echo "✓ Deploy completo."
echo ""
echo "Próximos pasos:"
echo "  1. Correr el job una vez para poblar datos:"
echo "     databricks bundle run daily_pipeline --target $TARGET"
echo ""
echo "  2. Esperar a que Vector Search termine el sync (5-10 min)"
echo ""
echo "  3. Abrir el App:"
echo "     databricks apps get hansard-cr-app | grep url"
