{% docs bridge_questions_tag_vw %}

# bridge_questions_tag_vw

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

- `question_id` — Question identifier (FK to `stg_so_questions`).  
- `tag_id` — Surrogate key of the normalized tag (FK to `dim_tags`).

**Business logic summary**

1. For each question, explode `tags_array` into individual tags.  
2. Normalize tag text (`LOWER(TRIM)`), drop blanks, `SELECT DISTINCT`.  
3. Map normalized `tag` to `tag_id` via `dim_tags`.  
4. Emit `(question_id, tag_id)` pairs only (no raw tag strings).

**Why this exists**

- Powers **tag-based slicing** (e.g., unanswered by tag, activity by tag).  
- Provides a **clean, deduplicated** join path for fact tables to tag attributes and metrics.

**Notes & caveats**

- Only tags present in `dim_tags` will appear; ensure `dim_tags` is built from the same normalization logic to avoid mismatches.  
- If you need raw tag text for display, join back to `dim_tags` on `tag_id`.

**Example usage**

```sql
-- Questions per tag (top 20)
SELECT dt.tag, COUNT(*) AS questions
FROM {{ ref('bridge_questions_tag_vw') }} b
JOIN {{ ref('dim_tags') }} dt USING (tag_id)
GROUP BY dt.tag
ORDER BY questions DESC
LIMIT 20;

-- Join questions to their tags for further enrichment
SELECT q.question_id, q.title, dt.tag
FROM {{ ref('questions_latest') }} q
JOIN {{ ref('bridge_questions_tag_vw') }} b USING (question_id)
JOIN {{ ref('dim_tags') }} dt USING (tag_id);
