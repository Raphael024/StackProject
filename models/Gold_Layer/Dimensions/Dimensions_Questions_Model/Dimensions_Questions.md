{% docs Dimension_questions %}

#Dimension_questions#

**Purpose**

`Dimension_questions` de-duplicates questions from the staging model and returns **one latest record per `question_id`**.  
It prefers rows with the most recent `last_activity_ts` and, as a tie-breaker, the most recent `creation_ts`.

**Business logic (summary)**

- Start from `stg_so_questions`.
- Keep only rows with non-null `question_id`.
- Construct a canonical `question_url`:
  - Use `q.question_url` if present,
  - Otherwise build `https://stackoverflow.com/questions/<question_id>`.
- Use `ROW_NUMBER()` over `question_id` ordered by:
  1. `last_activity_ts` DESC (NULLS LAST),
  2. `creation_ts` DESC (NULLS LAST),
  and keep the first row.

**Model grain**

- **One row per `question_id`**.

**Columns**

- `question_id` — Primary key for the model.
- `title` — Latest title associated with the question.
- `question_url` — Canonical URL for the question; always populated.

**Upstream dependencies**

- `Silver_Layer_Questions` (staging view/table containing raw Stack Overflow questions with timestamps).

**Why this exists**

Analytics and reporting often need a **single, most-up-to-date view** of each question (for scorecards, counts, joins to answers/tags, and landing pages). This model guarantees uniqueness and consistency.

**Assumptions & caveats**

- If `last_activity_ts` is null for multiple rows of the same `question_id`, ordering falls back to `creation_ts`.
- Titles can change over time; this model surfaces the title from the latest row according to the ordering above.
- URLs are normalized; when missing upstream, they are synthesized reliably from `question_id`.

**Quality checks **

- `question_id` is `not_null` and `unique`.
- `question_id` has a `relationships` test back to `stg_so_questions.question_id`.
- `title` and `question_url` are `not_null`.

{% enddocs %}