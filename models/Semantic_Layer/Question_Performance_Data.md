{% docs Question_Performance_Data %}

# Question_Performance_Data 

**Purpose**

A **semantic-layer wide view** that combines question facts, user attributes, accepted answer details, tags, and date lookups into one question-grain dataset.  
This model powers downstream analytics and reporting without requiring multiple joins.

**Lineage**

- **facts_questions_vw** → base measures & flags (answers, views, score, favorites).  
- **dim_questions** → title, URL.  
- **dim_users** → asker & accepted answer user attributes.  
- **facts_answers_vw** → accepted answer resolution.  
- **bridge_questions_tag_vw** + **dim_tags** → tag arrays, counts, CSV.  
- **dim_date** → date keys and actual date lookups for creation and last activity.

**Grain**

- **One row per `question_id`.**

**Major groups of fields**

- **Question attributes:** `title`, `question_url`.  
- **Asker details:** `asker_user_id`, `asker_name`, `asker_reputation`, `asker_country`.  
- **Dates:** `creation_dt`, `last_activity_dt`, `creation_month`, `creation_year`, `creation_month_num`, `creation_quarter`, with surrogate keys.  
- **Metrics:** `answer_count`, `view_count`, `score`, `favorite_count`.  
- **Flags:** `has_answers`, `has_accepted_answer`.  
- **Accepted answer details:** IDs, author, author reputation.  
- **Tags:** `tag_count`, `tags_array`, `tags_csv`, `tag_ids`.

**Business logic highlights**

- Uses **dim_date** for canonical date derivation — avoids relying on raw silver timestamps.  
- Ensures **no fan-out** by pre-aggregating tags and accepted answers to question grain.  
- Retains both raw accepted answer ID (`accepted_answer_id_from_q`) and resolved info from answers/users.  
- Provides multiple tag formats (array, CSV, IDs) for flexible slicing.

**Why this exists**

This view consolidates all key question-related entities into one **performance reporting layer**:  
- Enables rich BI dashboards without complex joins.  
- Provides canonical metrics for KPIs (engagement, acceptance, tagging coverage).  
- Forms the basis of trend, cohort, and cross-sectional analysis at the question grain.

{% enddocs %}