# sherlock-backend

**AI report generation engine with live entity intelligence** — takes a colloquial police incident description, cross-references every name, plate, and location against a records database in real time, then generates a properly formatted official Catalan police report with annotated highlights.

The core insight: a patrol officer shouldn't have to manually check if a person they're dealing with has a prior record. Sherlock does it in the background, inline, while the report is being written.

Part of the **Bitácola ecosystem**. Integrated into `bitacola-backend` as the AI processing service.

> ⚠️ This repository is **private**. It processes operational law enforcement data including a records database of persons and vehicles. This description is public for portfolio purposes.

---

## The operational problem it solves

An officer is dealing with an incident. They dictate what happened in plain language. Sherlock:

1. Detects every person, vehicle, and location mentioned in the text
2. Instantly cross-references each against the local records database
3. Shows the officer — before the report is even finished — whether that person is known, how many times they've appeared in previous incidents, and any flagged observations
4. Generates the official formatted report automatically

The officer knows immediately if the drunk in front of them is a known individual, if the van parked outside is registered to someone with prior incidents, or if that address has a history. All of this surfaced passively, without extra steps.

---

## Pipeline

```
Colloquial text (officer input)
         │
         ▼
┌─────────────────────────────────────┐
│  STEP 1 — Regex Extractor           │
│  Plates · DNIs · Names · Addresses  │
│  Catalan/Spanish stopword filter    │
│  Character-level positions tracked  │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  STEP 2 — Matcher (deterministic)   │
│  SQLite DB lookup (hermano_major)   │
│  Plates: exact match                │
│  Persons: token fuzzy (rapidfuzz)   │
│           returns all candidates    │
│  Locations: canonical name lookup   │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  STEP 3 — Marker Injection          │
│  [[VEHICLE:9915GBN|VW Golf|EXACTO]] │
│  [[PERSONA:Juan Garcia|BD:...|PARC]]│
│  [[PERSONA:Maria Puig||NOU]]        │
│  Injected right-to-left (no offset) │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  STEP 4 — DRAG Redactor (Claude)    │
│  Haiku · 60-400 words               │
│  Preserves [[...]] markers exactly  │
│  No legal classification            │
│  Catalan formal police register     │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  STEP 5 — Residual entity check     │
│  Catches entities Claude added      │
│  that weren't in original text      │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  STEP 6 — Annotator (deterministic) │
│  Reads [[...]] → strips markers     │
│  Generates character-level spans    │
│  for frontend inline highlighting   │
└─────────────────────────────────────┘
         │
         ▼
{
  texto_drag:  "clean report text",
  anotaciones: [{start, end, match, confidence, historial_count, db_data}],
  entidades:   {personas, vehiculos, ubicaciones with full DB data},
  ms:          340
}
```

---

## Entity matching design

**The name matching problem.** Police incidents involve names spoken aloud, transcribed imperfectly, often partial: "un tal Gangas", "la Maria Puig", "Juan Garcia". Simple string matching fails. Sherlock uses a token-based fuzzy pipeline:

1. Tokenize both the extracted name and the DB record — stripping stopwords (`del`, `de la`, `i`, `y`, connectives)
2. Build a token similarity matrix with `rapidfuzz`
3. Assign each search token to its best DB token (greedy, no reuse)
4. Penalize unmatched tokens
5. Return all candidates above a 70% threshold — not just the top match

This handles compound names (`Maria Del Carmen` → `[MARIA, CARMEN]`), partial apellidos (`Gangas` matching `Gangas Alvear`), and flexible token order.

**Marker types returned to the frontend:**

| Marker | Meaning | Frontend display |
|--------|---------|-----------------|
| `EXACTO` | Verified match — plate or DNI confirmed | Blue highlight |
| `PARCIAL` | Multiple candidates — officer must select | Amber highlight |
| `NOU` | Not in database — new entity | Grey highlight |

**The Catalan/Spanish stopword problem.** The regex name extractor must capitalize words to detect proper nouns — but colloquial police text is full of capitalized function words: `Ha`, `Le Han Robado El`, `Ha Venido Y Ha Dicho`. Without filtering, these become false "names". The extractor has an explicit bilingual stopword list (Catalan + Spanish) covering auxiliary verbs, articles, pronouns, and common legal/police terms.

---

## Key design decisions

**Markers survive Claude.** The system prompt explicitly instructs the redactor to copy `[[...]]` markers verbatim into the output. This is enforced by prompt design, not post-processing — Claude treats them as opaque tokens. The deterministic annotator then reads them out to generate the frontend spans.

**No legal classification in the redactor.** The system prompt explicitly prohibits any legal qualification (`presumpte delicte de...`, article references, `furt amb violència`). Criminal classification is handled by `penal_backend` as a separate pipeline. The two systems never interfere.

**Deterministic annotator.** The `AnnotatorService` contains zero AI calls. It reads the marker format with a regex, computes character positions in the clean text, resolves span overlap, and serializes to JSON. This makes the annotation layer fast, predictable, and testable.

**Fallback extractor.** If the Claude NER call fails, the system falls back to the regex extractor. Plates and DNIs are always caught; person names may be missed. The system degrades gracefully rather than failing.

**Legacy folder.** The `legacy/` directory contains the v2 orchestrator with a different architecture (Claude NER as the primary extractor). The current v4.1 moved NER to regex-first for speed and reliability, using Claude only for report generation.

---

## Database schema (hermano_major.db)

SQLite, read-only in production. Tables: `persons`, `vehicles`, `locations`, `entity_links` (incident history), `vehicle_person_links`, `person_roles`, `location_aliases`.

The same database is also accessible via `penal_backend` and `central-partes`.

---

## Output modes

| Mode | Length | Format |
|------|--------|--------|
| `parte` | 60–100 words | Shift log entry — concise chronological prose |
| `informe` | 250–400 words | Full DRAG-standard tipificat report |

---

## Stack

```
Python 3.11     Core language
FastAPI         REST API
Anthropic API   claude-3-haiku (report generation) · claude-3-sonnet (NER, legacy)
SQLite          Records database (read-only)
rapidfuzz       Token fuzzy matching for names
unidecode       Unicode normalization for Catalan/Spanish text
Pydantic        Request/response validation
```

**Deployment:** Render.com. Kept alive by keepalive pings from `bitacola-backend` every 10 minutes.

---

## Related

- [`bitacola-backend`](https://github.com/gienini2/bitacola-backend) — Auth proxy and frontend host; calls this service at `/api/v1/process`
- [`penal_backend`](https://github.com/gienini2/penal_backend) — Criminal code classification (called in parallel from the frontend)
- `central-partes` — Command panel aggregating reports across municipalities (separate private repo)
