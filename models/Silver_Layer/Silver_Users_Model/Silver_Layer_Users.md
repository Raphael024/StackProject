{% docs Silver_Layer_User %}

# Silver_Layer_User

**Purpose**

Clean, typed **staging view for Stack Overflow users**.  
It preserves the raw payload for traceability, enforces type safety, normalises textual attributes, and derives a lightweight country guess from freeform location data.

**Lineage**

- **Source:** `{{ source('so_raw','v_users') }}`

**Key transformations**

- **Raw retention:** Original row kept in `raw_record` for audit/debug.  
- **Type safety:** `user_id`, `reputation` cast to INT64; date fields cast to TIMESTAMP and DATE.  
- **Text cleanup:** `display_name` and `location` trimmed; empty strings replaced with NULL.  
- **Country guess:** Splits `location` by comma and takes the last element as a proxy for country.  
- **Dates:** `join_ts`/`join_dt` from `creation_date`; `last_access_ts`/`last_access_dt` from `last_access_date`.

**Grain**

- **One row per `user_id`.**

**Columns**

- `raw_record` — Full raw user payload.  
- `user_id` — Surrogate key for the user.  
- `display_name` — User’s visible handle (nullable).  
- `reputation` — Reputation score (non-negative).  
- `location` — Raw location text.  
- `country_guess` — Simple heuristic extraction of the country from location.  
- `join_ts`, `join_dt` — User account creation timestamp/date.  
- `last_access_ts`, `last_access_dt` — Most recent access timestamp/date.

**Notes & caveats**

- `country_guess` is **heuristic only**; relies on comma-delimited formatting and may be inaccurate.  
- Reputation is assumed non-negative; enforced via tests.  
- Dates are safely cast; malformed raw dates yield NULL without breaking the model.

**Example usage**

```sql
-- Count of active users by country
SELECT country_guess, COUNT(*) AS users
FROM {{ ref('Silver_Layer_User') }}
WHERE last_access_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY country_guess
ORDER BY users DESC;

-- Identify top reputation users
SELECT display_name, reputation
FROM {{ ref('Silver_Layer_User') }}
ORDER BY reputation DESC
LIMIT 20;
