{% docs Facts_Questions %}

# Facts_Questions

**Purpose**

A **fact view for questions** on Stack Overflow.  
It standardizes canonical columns, adds calendar joins, and pre-aggregates the number of answers per question—so downstream models can analyze question activity, engagement, and acceptance without fan-out.

**Lineage**

- **Questions source:** `stg_so_questions` → core question fields (IDs, dates, views, score, favorites, accepted answer).  
- **Answers source:** `stg_so_answers` → pre-aggregated `answer_count` per question.  
- **Calendar dimension:** `dim_date` → date surrogate keys and month buckets.

**Grain**

- **One row per `question_id`.**

**Columns**

- `question_id` — Primary key for the model.  
- `asker_user_id` — Author of the question.  
- `accepted_answer_id` — ID of the accepted answer (nullable).  
- `creation_dt`, `last_activity_dt` — Canonical DATE fields.  
- `creation_month` — Month bucket for time-series aggregations.  
- `creation_date_key`, `last_activity_date_key` — Join keys to `dim_date` (YYYYMMDD).  
- `answer_count` — Distinct answer count (pre-aggregated to avoid joins causing duplication).  
- `view_count` — Total views for the question.  
- `score` — Net score (upvotes - downvotes).  
- `favorite_count` — Count of favorites.  
- `has_answers` — TRUE if `answer_count > 0`.  
- `has_accepted_answer` — TRUE if `accepted_answer_id` is not NULL.

**Business logic summary**

1. Filter out rows with NULL `question_id`.  
2. Aggregate answers to question grain using `COUNT(DISTINCT answer_id)`.  
3. Join calendar dimension for date keys and compute `creation_month` via `DATE_TRUNC`.  
4. Emit flags for quick filtering: `has_answers`, `has_accepted_answer`.

**Why this exists**

- Prevents **fan-out** when joining questions to answers by doing the answer count upstream.  
- Provides a **single, consistent** source for question-level metrics used by dashboards and semantic layers.  
- Simplifies time-series analysis with ready-made month buckets and date keys.

{% enddocs %}
