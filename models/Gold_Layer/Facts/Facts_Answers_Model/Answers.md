{% docs facts_answers_vw %}

# facts_answers_vw

**Purpose**

A **fact view for answers** on Stack Overflow.  
Provides cleaned, canonicalized columns with foreign key references to questions and the calendar dimension, enabling analysis of answer activity and acceptance.

**Lineage**

- **Answers source**: `stg_so_answers` → basic answer fields.  
- **Questions source**: `stg_so_questions` → acceptance link.  
- **Calendar dimension**: `dim_date` → surrogate keys for creation and last activity dates.

**Grain**

- **One row per `answer_id`.**

**Columns**

- `answer_id` — Primary key for answers.  
- `question_id` — Foreign key back to the related question.  
- `answerer_user_id` — User ID of the author.  
- `creation_dt` — Date the answer was created.  
- `last_activity_dt` — Date the answer last had activity.  
- `creation_month` — Month bucket for grouping answers by month.  
- `creation_date_key` — Join key to `dim_date` for creation_dt.  
- `last_activity_date_key` — Join key to `dim_date` for last_activity_dt.  
- `score` — Net score (upvotes minus downvotes).  
- `comment_count` — Number of comments on the answer.  
- `is_accepted` — TRUE if this is the accepted answer for its question.

**Business logic summary**

- Answers with `NULL` IDs are excluded.  
- Accepted flag is computed by checking whether `answer_id = accepted_answer_id` from `stg_so_questions`.  
- Calendar joins add surrogate keys and month-level bucketing.  
- View materialization ensures flexibility for downstream aggregates.

**Why this exists**

Provides a **ready-to-use fact table** for reporting on answers:  
- Counts and distributions over time.  
- Accepted vs non-accepted analysis.  
- Score and engagement trends.  
- Joins to questions and tags via `question_id`.

**Example usage**

```sql
-- Find acceptance rate of answers
SELECT
  COUNTIF(is_accepted) / COUNT(*) AS acceptance_rate
FROM {{ ref('facts_answers_vw') }};

-- Average score of accepted vs non-accepted answers
SELECT
  is_accepted,
  AVG(score) AS avg_score
FROM {{ ref('facts_answers_vw') }}
GROUP BY is_accepted;

-- Answers per month
SELECT
  creation_month,
  COUNT(*) AS answers
FROM {{ ref('facts_answers_vw') }}
GROUP BY creation_month
ORDER BY creation_month;
