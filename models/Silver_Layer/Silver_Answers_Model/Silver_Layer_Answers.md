{% docs Silver_Layer_Answer %}

# Silver_Layer_Answer

**Purpose**

Clean, typed **staging view for Stack Overflow answers**.  
It preserves the full raw payload for traceability, adds typed keys and dates, normalizes identity fields, and guarantees a canonical `answer_url`.

**Lineage**

- **Source:** `{{ source('so_raw','v_answers') }}`

**Key transformations**

- **Raw retention:** Entire source row kept under `raw_record` (STRUCT) for audit/debug.
- **Type safety:** Keys cast to `INT64`; dates to both `TIMESTAMP` and `DATE` via `SAFE_CAST`.
- **Display name hygiene:** `NULLIF(TRIM(owner_display_name),'')` to avoid empty strings.
- **Canonical URL:** `COALESCE(answer_url, 'https://stackoverflow.com/a/<answer_id>')`.
- **Booleans:** `is_accepted` cast to `BOOL`.

**Grain**

- **One row per `answer_id`.**

**Columns**

- `raw_record` — Full raw payload as STRUCT.  
- `answer_id`, `question_id` — Integer keys.  
- `creation_ts` / `creation_dt`, `last_activity_ts` / `last_activity_dt`, `last_edit_ts` / `last_edit_dt` — Typed temporal fields.  
- `score`, `comment_count` — Engagement metrics.  
- `answerer_user_id`, `answerer_display_name` — Author metadata.  
- `answer_url` — Canonical link, always populated.  
- `is_accepted` — Whether the answer is accepted.

**Notes & caveats**

- `score` can be negative; no non-negativity test is applied.  
- `comment_count` is constrained to be ≥ 0.  
- If upstream emits malformed dates, `SAFE_CAST` yields `NULL` (surfaces data quality issues without failing the load).

**Example usage**

```sql
-- Recently edited accepted answers
SELECT answer_id, question_id, last_edit_dt, score
FROM {{ ref('Silver_Layer_Answer') }}
WHERE is_accepted = TRUE
  AND last_edit_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

-- Answers with missing author display names (for cleanup)
SELECT answer_id, answerer_user_id
FROM {{ ref('Silver_Layer_Answer') }}
WHERE answerer_display_name IS NULL;
