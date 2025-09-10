{% docs dim_date %}

# dim_date

**Purpose**

A standard calendar dimension spanning from **2010-01-01 to `CURRENT_DATE()`** at run time.  
Provides common date parts and flags for robust time-series modeling and reporting.

**Grain**

- **One row per calendar day**.

**Key columns**

- `date_key` — Integer surrogate key in `YYYYMMDD` (e.g., 20250309).  
- `date` — Native DATE.  
- `year`, `quarter`, `month`, `day` — Calendar components derived from `date`.  
- `iso_week` — ISO week number (1–53).  
- `dow` — BigQuery `DAYOFWEEK` numbering: **Sunday=1 … Saturday=7**.  
- `is_weekend` — TRUE when `dow` in (1,7).

**Logic summary**

1. Build a continuous series of dates using `GENERATE_DATE_ARRAY('2010-01-01', CURRENT_DATE())`.  
2. Derive parts via `EXTRACT(...)`.  
3. Create `date_key` with `FORMAT_DATE('%Y%m%d', date)` cast to INT64.  
4. Weekend flag where `DAYOFWEEK IN (1,7)`.

**Notes & caveats**

- `iso_week` uses ISO-8601 week semantics via `EXTRACT(ISOWEEK FROM date)`.  
- If you need an ISO-aligned **week year** (e.g., for dates near year boundaries), add `EXTRACT(ISOYEAR FROM date)` to avoid grouping mismatches.  
- The calendar starts at 2010-01-01; adjust the start date if historical reporting requires more coverage.

**Example usage**

```sql
-- Typical date join on surrogate key
SELECT f.*, d.year, d.month, d.iso_week
FROM {{ ref('some_fact') }} f
JOIN {{ ref('dim_date') }} d
  ON d.date_key = f.date_key;

-- Filter to business days (Mon–Fri)
SELECT *
FROM {{ ref('dim_date') }}
WHERE dow BETWEEN 2 AND 6;

-- Year-to-date date set
SELECT *
FROM {{ ref('dim_date') }}
WHERE date BETWEEN DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 1, 1) AND CURRENT_DATE();
