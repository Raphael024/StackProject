{% docs Bridge_Questions_Tag %}

# Bridge_Questions_Tag

**Purpose**

A **many-to-many bridge** that links Stack Overflow questions to their tags.  
Each row represents one `(question_id, tag_id)` pair, enabling joins from questions/answers to tag-level analytics.

**Lineage**

- **Source questions:** `stg_so_questions`
  - Explodes `tags_array` with `UNNEST()`.
  - Normalizes tags with `LOWER(TRIM(...))`.
  - Filters out empty strings and de-duplicates pairs.
- **Tag dimension:** `dim_tags`
  - Provides stable `tag_id` for each normalized `tag`.

**Grain**

- **One row per unique `(question_id, tag_id)`**.

**Columns**

- `question_id` — Question identifier (FK to `Silver_Layer_Questions`).  
- `tag_id` — Surrogate key of the normalized tag (FK to `Dimension_Tags`).

**Business logic summary**

1. For each question, explode `tags_array` into individual tags.  
2. Normalize tag text (`LOWER(TRIM)`), drop blanks, `SELECT DISTINCT`.  
3. Map normalized `tag` to `tag_id` via `dim_tags`.  
4. Emit `(question_id, tag_id)` pairs only (no raw tag strings).

**Why this exists**

- Powers **tag-based slicing** (e.g., unanswered by tag, activity by tag).  
- Provides a **clean, deduplicated** join path for fact tables to tag attributes and metrics.

**Notes & caveats**

- Only tags present in `dim_tags` will appear; ensure `Dimension_Tags` is built from the same normalization logic to avoid mismatches.  
- If you need raw tag text for display, join back to `Dimension_Tags` on `tag_id`.

{% enddocs %}