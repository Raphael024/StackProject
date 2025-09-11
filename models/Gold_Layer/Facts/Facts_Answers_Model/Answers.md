{% docs Answers %}

# Answers

**Purpose**

A **fact view for answers** on Stack Overflow.  
Provides cleaned, canonicalized columns with foreign key references to questions and the calendar dimension, enabling analysis of answer activity and acceptance.

**Lineage**

- **Answers source**: `Silver_Layer_Answer` → basic answer fields.  
- **Questions source**: `Silver_Layer_Questions` → acceptance link.  
- **Calendar dimension**: `Dimension_Date` → surrogate keys for creation and last activity dates.

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
- Accepted flag is computed by checking whether `answer_id = accepted_answer_id` from `Silver_Layer_Questions`.  
- Calendar joins add surrogate keys and month-level bucketing.  
- View materialization ensures flexibility for downstream aggregates.

**Why this exists**

Provides a **ready-to-use fact table** for reporting on answers:  
- Counts and distributions over time.  
- Accepted vs non-accepted analysis.  
- Score and engagement trends.  
- Joins to questions and tags via `question_id`.

{% enddocs %}