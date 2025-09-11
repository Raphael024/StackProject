{% docs Silver_Layer_Tags %}

# Silver_Layer_Tags

**Purpose**

Clean, typed **staging view for Stack Overflow tags**.  
It preserves the full raw payload, normalises the tag key, and casts attributes to stable types for downstream use.

**Lineage**

- **Source:** {% raw %}{{ source('DBT_RAW','v_Tag') }}{% endraw %}

**Key transformations**

- **Raw retention:** Entire source row kept as `raw_record` (STRUCT) for audit/debug.
- **Normalisation:** `tag` is `LOWER(TRIM(raw tag))` to ensure join-safe keys.
- **Type safety:** Numeric attributes are `SAFE_CAST` to `INT64`:
  - `tag_count_raw`, `excerpt_post_id`, `wiki_post_id`.

**Grain**

- **One row per normalised `tag`.**  
  (If the raw feed contains case/whitespace variants, they collapse into a single normalised key.)

**Columns**

- `raw_record` — Full raw payload as STRUCT.  
- `tag` — Normalised tag (lowercased, trimmed).  
- `tag_count_raw` — Community-reported usage count (non-negative).  
- `excerpt_post_id` — Excerpt post reference (nullable).  
- `wiki_post_id` — Wiki post reference (nullable).

**Notes & caveats**

- The model **does not** perform synonym/alias mapping; that belongs in downstream dimensions (e.g., `dim_tags`).
- `SAFE_CAST` yields `NULL` for malformed numbers, surfacing data quality issues without failing the run.
- If you later hash to a surrogate key (e.g., `FARM_FINGERPRINT`), use this normalised `tag` as the input.

{% enddocs %}
