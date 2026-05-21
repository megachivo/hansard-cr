# 01 — Contexto: fuentes de datos verificadas

## Resumen ejecutivo

Costa Rica **no tiene API pública de datos legislativos** como Argentina (`datos.hcdn.gob.ar`) o Brasil (`dadosabertos.camara.leg.br`). La información del Plenario está dispersa en 3 silos, ninguno con búsqueda full-text ni endpoints estructurados. Este es el espacio en blanco que llena Hansard CR.

## Fuentes confirmadas

### 1. Actas oficiales (texto autoritativo)

**Plenario:**
- Índice principal: `https://www.asamblea.go.cr/glcp/actas/forms/plenario.aspx`
- Actas provisionales / faltantes: `https://www.asamblea.go.cr/glcp/prov_actas/forms/allitems.aspx`
- Las actas son SharePoint document libraries con PDFs.

**Jefaturas de fracción** (útil para contexto político):
- `https://www.asamblea.go.cr/glcp/actas_jefaturas/...`

**Espejo histórico en CONARE** (HTML más limpio que el PDF, útil para histórico):
- `https://proyectos.conare.ac.cr/asamblea/`

**Latencia:** las actas escritas se publican con semanas o meses de retraso vs la sesión real. Por eso necesitamos YouTube.

### 2. YouTube — canal oficial (la fuente fresca)

- Canal: `https://www.youtube.com/@AsambleaCRC`
- Patrón de títulos: `Plenario Legislativo, sesión ordinaria #96, 03 febrero 2025`
- Patrón de títulos extraordinarias: `Plenario Legislativo, sesión extraordinaria #80, martes 30 abril 2024`
- Sesiones solemnes: `Plenario Legislativo, sesión solemne 1 de mayo 2024`
- Duración típica: 4–6 horas por sesión.
- **Esta es la fuente más fresca.** Sale el mismo día.

### 3. SIL — Sistema de Información Legislativa (metadata estructurada)

- Portal: `https://www.asamblea.go.cr/Centro_de_informacion/Consultas_SIL/SitePages/Inicio.aspx`
- Contiene: expedientes, proyectos de ley, diputados, comisiones.
- **No expone API.** Hay que scrapear formularios ASP.NET (con `__VIEWSTATE`, `__EVENTVALIDATION`). Usar Playwright si requests no alcanza.
- Para el demo: solo necesitamos el listado de los 57 diputados activos + sus fracciones. No el SIL completo.

### 4. Ecosistema existente (para no reinventar)

- `https://delfino.cr/asamblea/legislacion` — medio digital que ya scrappea estado de expedientes. **No transcribe video, no hace búsqueda semántica.** Ese es nuestro hueco.

## Lo que NO existe (y por qué este proyecto importa)

- ❌ API de datos abiertos legislativos
- ❌ Buscador full-text sobre actas
- ❌ Endpoint estructurado de votaciones nominales (están como texto dentro del PDF)
- ❌ Transcripción de los videos
- ❌ Análisis comparativo entre diputados / temas / períodos

## Legalidad

- Actas son documentos públicos (Constitución arts. 28 y 30).
- Video del canal oficial es público.
- Ley 8968 de Protección de Datos no aplica a actos legislativos públicos.
- Posible cuidado: datos personales incidentales mencionados en debates (cédulas, direcciones). Ofuscar con regex en post-proceso si aparecen.

## Riesgos de las fuentes

| Riesgo | Mitigación |
|--------|------------|
| Sitio de la Asamblea cambia de URL | Tenemos 3 fuentes redundantes; CONARE como respaldo histórico |
| SharePoint requiere autenticación en algunos casos | Probar acceso anónimo primero; si falla, ir directo a YouTube |
| Whisper falla con vocabulario tico (CCSS, RECOPE, JASEC, ICE) | Post-proceso con diccionario de siglas; modelo `large-v3` |
| Speaker diarization es lenta y cara | Saltarla en el demo; usar regex sobre actas para asociar texto↔diputado |
| Robots.txt o rate limits | Bajar PDFs con `time.sleep(2)` entre requests; cachear todo en `bronze` |
