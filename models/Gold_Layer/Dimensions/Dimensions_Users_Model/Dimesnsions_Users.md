{% docs questions_latest %}

# questions_latest

**Purpose**

Returns **one latest record per `question_id`** from `stg_so_questions`, preferring the row with the most recent `last_activity_ts` and, as a tie-breaker, the most recent `creation_ts`. Ensures a canonical `question_url`.

**Logic (summary)**

1. Select questions with non-null `question_id`.
2. Build `question_url` using the upstream value if present, otherwise:
   `https://stackoverflow.com/questions/<question_id>`.
3. Rank rows within each `question_id` by:
   - `last_activity_ts` DESC (NULLS LAST),
   - `creation_ts` DESC (NULLS LAST).
4. Keep `rn = 1`.

**Grain**

- **One row per `question_id`** (the latest by activity/creation).

**Columns**

- `question_id` — Primary key for this model.  
- `title` — Latest title associated with the selected row.  
- `question_url` — Canonical URL; always populated.

**Upstream dependency**

- `stg_so_questions` (staging model of Stack Overflow questions).

**Assumptions & notes**

- If `last_activity_ts` is NULL for all rows in a group, ordering falls back to `creation_ts`.  
- Titles can change over time; the surfaced title corresponds to the chosen latest row.

**Example usage**

```sql
-- Count distinct questions
select count(*) from {{ ref('questions_latest') }};

-- Join to bridges or facts
select ql.question_id, ql.title, b.tag_id
from {{ ref('questions_latest') }} ql
left join {{ ref('bridge_questions_tag') }} b
  on b.question_id = ql.question_id;
