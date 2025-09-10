{% docs dim_tags %}

# dim_tags

**Purpose**

A canonical **dimension of Stack Overflow tags**, produced by normalising tag strings found on questions and enriching them with attributes from the tags catalog. Each row is **one unique tag** (lowercased, trimmed), with a **stable deterministic `tag_id`** for downstream joins.

**Lineage**

- **From questions:** `stg_so_questions`  
  - Explodes `tags_array`, normalises with `LOWER(TRIM(...))`, and **de-duplicates**.
- **From tag catalog:** `stg_so_tags`  
  - Brings `tag_count_raw`, `excerpt_post_id`, and `wiki_post_id` (when available).

**Key logic**

- **Deterministic ID:** `tag_id = CAST(ABS(FARM_FINGERPRINT(tag)) AS INT64)`  
  - BigQuery’s `FARM_FINGERPRINT` yields a consistent hash for the normalised `tag`.  
  - The `ABS(...)` + `INT64` ensures a non-negative 64-bit integer that’s stable across runs.
- **Allowed characters check:** `has_illegal_chars` uses regex `[^a-z0-9\-\+\#\.]`  
  - Allowed: lowercase letters, digits, hyphen (`-`), plus (`+`), hash (`#`), dot (`.`).  
  - Useful for **data hygiene** and surfacing tags that may need cleaning or mapping.
- **Coverage flag:** `is_zero_count` marks tags with missing/zero `tag_count_raw` so you can filter or QA.

**Grain**

- **One row per normalised `tag`.**

**Columns**

- `tag_id` — Stable integer surrogate key derived from the normalised tag string.  
- `tag` — The normalised tag key (lowercase, trimmed).  
- `tag_count_raw` — Community-reported frequency; `0` when unavailable.  
- `excerpt_post_id` — Reference to the tag’s excerpt page (nullable).  
- `wiki_post_id` — Reference to the tag’s wiki page (nullable).  
- `has_illegal_chars` — TRUE if the tag includes characters outside the allowed set.  
- `is_zero_count` — TRUE if `tag_count_raw` is zero or missing upstream.

**Why this exists**

- Provides a **clean, unique** tag dimension for joining fact tables (e.g., question/answer bridges) and building **tag-level analytics** (popularity, hygiene checks, trend slice keys).

**Assumptions & caveats**

- The tag normalisation is **lowercase + trim only**; no remapping of synonyms/aliases is performed here.  
- `tag_count_raw` origin and semantics depend on the upstream catalog; treat it as **indicative**, not authoritative.  
- The regex is a **strict** allowlist; adjust if your business rules permit additional characters.

**Example usage**

```sql
-- Join facts by tag_id after mapping tags to IDs via dim_tags
WITH tags AS (
  SELECT question_id, LOWER(TRIM(tag)) AS tag
  FROM {{ ref('stg_so_questions') }}, UNNEST(tags_array) AS tag
)
SELECT
  t.question_id,
  dt.tag_id,
  dt.tag,
  dt.tag_count_raw
FROM tags t
JOIN {{ ref('dim_tags') }} dt
  ON dt.tag = t.tag;

-- Find tags that likely need cleanup
SELECT tag, has_illegal_chars
FROM {{ ref('dim_tags') }}
WHERE has_illegal_chars = TRUE
ORDER BY tag;
