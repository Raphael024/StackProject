{% docs Silver_Layer_Questions %}

# Silver_Layer_Questions

**Purpose**

A clean, typed **staging view for Stack Overflow questions**.  
This model preserves the raw payload for traceability, enforces type safety, normalises tags, constructs canonical URLs, and enriches measures/flags like `is_answered`.

**Lineage**

- **Source:** {% raw %}{{ source('DBT_RAW','v_questions') }}{% endraw %}

**Key transformations**

- **Raw retention:** Original row preserved as `raw_record` (STRUCT).  
- **Type safety:** IDs cast to `INT64`; dates cast to `TIMESTAMP` and `DATE`.  
- **Tags:** `tags_raw` kept as delivered; `tags_array` derived as lowercased, trimmed values split on `|`.  
- **Canonical URL:** Falls back to `https://stackoverflow.com/questions/<id>` if `question_url` missing.  
- **Answer flags:** `is_answered` derived from raw flag OR if `accepted_answer_id` is present OR `answer_count > 0`.  
- **Engagement:** View, score, favorite, comment counts cast and cleaned.  
- **Asker:** User ID and display name normalised.

**Grain**

- **One row per `question_id`.**

**Columns**

- `raw_record` — Full raw payload.  
- `question_id` — Primary key.  
- `title` — Question title (trimmed).  
- `tags_raw` — Original pipe-delimited string of tags.  
- `tags_array` — Clean array of lowercased tags.  
- `question_url` — Canonical URL.  
- `creation_ts` / `creation_dt` — Creation timestamp/date.  
- `last_activity_ts` / `last_activity_dt` — Last activity timestamp/date.  
- `last_edit_ts` / `last_edit_dt` — Last edit timestamp/date.  
- `accepted_answer_id` — Accepted answer ID (nullable).  
- `is_answered` — Boolean flag for answer presence.  
- `answer_count` — Number of answers.  
- `comment_count` — Number of comments.  
- `favorite_count` — Number of times favorited.  
- `score` — Net score.  
- `view_count` — Total views.  
- `asker_user_id` — ID of the asker.  
- `asker_display_name` — Display name of the asker.

**Notes & caveats**

- `score` may be negative (no constraint).  
- `view_count`, `answer_count`, `comment_count`, `favorite_count` enforced to be ≥ 0.  
- `tags_array` uses `LOWER(TRIM())`; synonyms/aliases not resolved here.  
- `is_answered` logic provides a fallback even if raw `is_answered` is missing.

{% enddocs %}