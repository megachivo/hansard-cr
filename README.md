# Hansard CR

Buscador del Plenario de la Asamblea Legislativa de Costa Rica. Combina actas oficiales (PDF) + transcripción automática de YouTube + LLM con citas. Todo sobre Databricks.

## Inicio rápido para Claude Code

1. Abrí Claude Code en este directorio. El archivo raíz **`CLAUDE.md`** orienta al agente.
2. Leé los docs en orden: `docs/01-contexto.md` → `02-arquitectura.md` → `03-plan-4-horas.md` → `04-pitch.md` → `05-fuentes-urls.md`.
3. Verificá prerequisitos (workspace Databricks, Apps habilitado, Foundation Model APIs).
4. Editá `databricks.yml` con tu workspace host y warehouse ID.

## Comandos típicos

```bash
# Setup inicial (una vez)
databricks configure
databricks bundle validate

# Deploy completo
./scripts/deploy.sh dev

# Correr pipeline una vez
databricks bundle run daily_pipeline

# Logs del App
databricks apps logs hansard-cr-app

# Re-deploy después de editar el app
databricks bundle deploy --target dev
```

## Cómo trabajar con Claude Code en este proyecto

Sugerencias de prompts:

- *"Leé `docs/01-contexto.md` y `docs/05-fuentes-urls.md`. Completá la función `listar_pdfs_disponibles` en `notebooks/01_scrape_actas.py`."*
- *"Leé `docs/02-arquitectura.md`. Implementá `segmentar_intervenciones` en `notebooks/01_scrape_actas.py` siguiendo el esquema descrito."*
- *"Probá las 3 queries del demo descritas en `CLAUDE.md`. Reportame cuál falla."*
- *"Optimizá `app/app.py` para que cargue en menos de 3 segundos."*

## Reglas para que Claude Code no se vaya por las ramas

1. **No invente URLs.** Use solo las de `docs/05-fuentes-urls.md`.
2. **No proponga cambios de stack.** El stack está decidido en `docs/02-arquitectura.md`.
3. **No agregue features que no estén en el plan.** El plan está en `docs/03-plan-4-horas.md`.
4. **Citas obligatorias en cualquier output de LLM.** No-negociable.
5. **Foco en las 3 queries de demo.** Si algo no las hace funcionar, no es prioridad.
