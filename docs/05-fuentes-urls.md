# 05 — Fuentes y URLs canónicas

> Pegá aquí TODAS las URLs verificadas. Claude Code debe usar SOLO estas.

## Asamblea Legislativa — sitio oficial

| Recurso | URL |
|---------|-----|
| Sitio raíz | `https://www.asamblea.go.cr/` |
| Actas Plenario | `https://www.asamblea.go.cr/glcp/actas/forms/plenario.aspx` |
| Actas provisionales | `https://www.asamblea.go.cr/glcp/prov_actas/forms/allitems.aspx` |
| Actas jefaturas de fracción | `https://www.asamblea.go.cr/glcp/actas_jefaturas/` |
| SIL (Sistema de Información Legislativa) | `https://www.asamblea.go.cr/Centro_de_informacion/Consultas_SIL/SitePages/Inicio.aspx` |
| Reglamento (para parsing) | `https://www.asamblea.go.cr/ca/Reglamentos%20de%20la%20Asamblea/Reglamento_de_la_Asamblea_Legislativa.pdf` |

## YouTube — canal oficial

| Recurso | URL |
|---------|-----|
| Canal | `https://www.youtube.com/@AsambleaCRC` |
| Playlists (verificar manualmente al empezar) | `https://www.youtube.com/@AsambleaCRC/playlists` |
| Videos recientes | `https://www.youtube.com/@AsambleaCRC/videos` (clips cortos de noticias — NO contiene sesiones) |
| Sesiones del plenario (live streams) | `https://www.youtube.com/@AsambleaCRC/streams` ← usar esta en el pipeline |

### Ejemplos de URLs de sesiones (para usar como semilla en demo)
- `https://www.youtube.com/watch?v=6RvmcG2CzqQ` — Plenario ordinaria #96, 03 febrero 2025
- `https://www.youtube.com/watch?v=82f-NpJeKeE` — Plenario extraordinaria #52, 03 abril 2025
- `https://www.youtube.com/watch?v=OWyoa3zmIfg` — Solemne 1 mayo 2025

### Patrón de títulos para parseo
```python
import re

PATTERN = re.compile(
    r"Plenario Legislativo,\s+sesión\s+"
    r"(?P<tipo>ordinaria|extraordinaria|solemne)"
    r"(?:\s+#(?P<numero>\d+))?,?\s+"
    r"(?:lunes|martes|miércoles|jueves|viernes|sábado|domingo)?\s*"
    r"(?P<dia>\d+)\s+(?:de\s+)?(?P<mes>\w+)\s+(?:de\s+)?(?P<anio>\d{4})",
    re.IGNORECASE,
)
```

## Espejo histórico (respaldo)

| Recurso | URL |
|---------|-----|
| CONARE — actas en HTML | `https://proyectos.conare.ac.cr/asamblea/` |

## Ecosistema (no scrapear, solo referenciar)

| Recurso | URL |
|---------|-----|
| Delfino — estado de expedientes | `https://delfino.cr/asamblea/legislacion` |

## Headers recomendados para scraping

```python
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-CR,es;q=0.9,en;q=0.8",
}
```

## Rate limiting

- `time.sleep(2)` entre PDFs.
- `time.sleep(5)` entre videos en yt-dlp.
- Si recibís 429 o 503, exponential backoff: 5 → 15 → 60 segundos.

## Reglamento de la Asamblea — datos útiles para el dominio

- 57 diputados total.
- Sesiones del Plenario: lunes a jueves, generalmente desde 15:00.
- Comisiones Plenas: miércoles desde 17:05.
- 6 comisiones permanentes ordinarias + 14 especiales permanentes.
- Período legislativo: 4 años; cada legislatura empieza el 1 de mayo.
- Legislaturas actuales: 2022-2026 (presidente del 2026: ver al iniciar).
