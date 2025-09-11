{% docs Dimension_Date %}

# Dimension_Date

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

{% enddocs %}