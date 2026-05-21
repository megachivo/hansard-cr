# 04 — Pitch para los jueces (5 minutos)

## Estructura

### Minuto 1 — El problema, concreto

> *"En Costa Rica, la última acta del Plenario publicada es de hace [X semanas]. Si un periodista o un ciudadano quiere saber qué dijo su diputado sobre la jornada 4x3, tiene que abrir 40 PDFs y hacer Ctrl-F. No hay buscador. No hay API. No hay datos abiertos."*

Mostrar el sitio actual de la Asamblea en pantalla. La fealdad vende.

**Comparar:** Argentina tiene `datos.hcdn.gob.ar`. Brasil tiene `dadosabertos.camara.leg.br`. Chile tiene `opendata.camara.cl`. Costa Rica no tiene nada.

### Minuto 2 — La demo, no la arquitectura

Tres queries en vivo:

1. **Buscador:** *"¿Qué se dijo sobre la CCSS esta semana?"*
   → resultados con video timestamped. Énfasis en que la fuente del video aún no tiene acta publicada — esto es **información que no existe en ningún otro lado**.

2. **Resumen:** *"Resumime la sesión del martes en 5 bullets."*
   → resumen automático con cada bullet linkeado a su intervención fuente.

3. **Comparativo (la que sorprende):** *"Comparame las posturas de [Diputado X] y [Diputada Y] sobre seguridad."*
   → side-by-side con citas.

### Minuto 3 — Por qué funciona técnicamente

Mostrar el diagrama **una sola vez, 30 segundos**.

Mensaje clave:

> *"Combinamos la fuente oficial escrita con transcripción automática del video oficial. Todo corre en Databricks: Delta + Unity Catalog gobierna las actas, Vector Search managed se sincroniza con cada nueva intervención, Whisper y el LLM son endpoints de Model Serving, y la UI es un Databricks App. No tenemos servidores. No inventamos datos. Cada respuesta cita su fuente, y cada cita es auditable contra el PDF original o el video oficial."*

Esto incluye dos puntos defensivos:
- **Anti-alucinación by design**, no por buena fe. El LLM solo ve contexto recuperado.
- **Gobierno desde día 1.** Unity Catalog = lineage de cada intervención.

### Minuto 4 — Impacto y por qué Costa Rica lo necesita

Tres usuarios concretos:

1. **Periodistas** (CRHoy, Semanario Universidad, Delfino, La Nación) — pueden encontrar declaraciones en segundos en vez de horas.
2. **Sociedad civil y academia** (Estado de la Nación, Programa de Transparencia UCR, observatorios) — análisis cuantitativo de patrones legislativos por primera vez.
3. **Diputados y sus asesores** — buscar precedentes, ver qué dijeron sus colegas, preparar intervenciones.

> *"Países comparables tienen esto desde hace años. Costa Rica está atrás. Hoy lo cerramos."*

### Minuto 5 — Sostenibilidad y cierre

No pretender que está terminado:

> *"En la próxima semana sumamos comisiones legislativas. En el próximo mes, votaciones nominales estructuradas. Modelo: open source, API freemium para medios, posibilidad de adopción directa por la Asamblea — los Asset Bundles permiten transferirlo a su workspace en 3 comandos."*

Cierre fuerte:

> *"El código está en GitHub. Hoy quedan abiertos los datos del Plenario de Costa Rica."*

---

## El minuto técnico — script literal (memorizar)

> "Esto corre 100% en Databricks. Las actas y las transcripciones de video viven en tablas Delta gobernadas por Unity Catalog. Cada nueva intervención dispara automáticamente la actualización del índice de Vector Search, que es managed. Para el LLM y los embeddings usamos Foundation Model APIs — no tenemos API keys de terceros sueltas. Whisper se expone como Model Serving endpoint con autoscale a GPU. Y todo lo que ven en pantalla es un Databricks App: 80 líneas de Streamlit, deploy en un comando. Si la Asamblea Legislativa quisiera adoptarlo mañana, le entregamos el Asset Bundle y queda corriendo en su workspace."

---

## Preguntas frecuentes de jueces y cómo responderlas

### "¿Esto es legal?"
Sí. Actas y video del canal son documentos públicos por Constitución (arts. 28 y 30). Ley 8968 de protección de datos no aplica a actos legislativos públicos. Si aparecen datos personales incidentales en debates, los ofuscamos con regex en post-proceso.

### "¿Y si la Asamblea cambia el sitio?"
Tenemos 3 fuentes redundantes: PDFs de SharePoint, canal de YouTube y SIL. YouTube es la más estable porque no depende del sitio de la Asamblea.

### "¿Whisper es preciso en español tico?"
El modelo `large-v3` tiene WER ~6-8% en español neutro. Con vocabulario tico (CCSS, RECOPE, JASEC, ICE) sube algo, pero post-procesamos con diccionario de siglas locales y nombres de diputados.

### "¿Por qué Databricks y no [stack X]?"
Tres razones:
1. **Gobierno desde día 1** vía Unity Catalog — crítico para datos públicos.
2. **Sin gestionar infra** — Vector Search managed, Serving managed, App managed.
3. **Reproducible** — Asset Bundles permiten que cualquier institución (la Asamblea, una universidad, una ONG) lo adopte sin migrar nada.

### "¿Cuál es el modelo de negocio?"
No es producto comercial; es infraestructura cívica. Sostenibilidad vía: (a) sponsoring de medios que ya cubren la Asamblea, (b) servicios pagos para análisis a medida (consultoras, asesores), (c) si la Asamblea adopta, ellos pagan el compute.

### "¿Cuánto cuesta operar esto?"
Estimación honesta:
- Whisper: ~$1.50/sesión (GPU, 5 horas de audio en ~30 min de procesamiento)
- Embeddings: ~$0.50/sesión
- Vector Search + Serving: ~$200/mes con tráfico moderado
- App compute: ~$50/mes
- **Total: <$500/mes** para cobertura completa del Plenario.

### "¿Por qué no usar OpenAI o Anthropic directo?"
Lo hacemos — vía Databricks External Models, que da una capa de gobierno, rate limiting y observabilidad encima. Una sola línea para cambiar de proveedor.

### "¿Qué pasa con las alucinaciones del LLM?"
No respondemos sin contexto recuperado. Cada output tiene citas linkeables. Si el contexto no alcanza, el prompt explícitamente le dice al modelo que conteste "no tengo suficiente contexto" — y lo cumple. Mostramos las fuentes siempre, debajo de la respuesta.

---

## Errores que NO hay que cometer

- ❌ Abrir con el diagrama de arquitectura. Los jueces se desconectan.
- ❌ Prometer features que no están en el prototipo.
- ❌ Demostrar el chat con una pregunta abierta que no probaste. Siempre falla.
- ❌ Hablar más del 20% del tiempo en stack técnico. Los jueces quieren ver producto.
- ❌ Decir "esto es solo un prototipo" en tono de disculpa. Es **un MVP funcional** — esa es la frase.

---

## El cierre, palabra por palabra

> *"Hoy es 21 de mayo de 2026. Hace 4 horas no existía. Ahora existe. Y desde ahora, en Costa Rica, lo que se dice en el Plenario se puede buscar."*

(Pausa. Sonrisa. Aplauso.)
