# Redshift Implementation Guide

> **Engine profile:** MPP (Massively Parallel Processing) columnar database. Data split across slices on compute nodes. Query optimizer uses statistics, zone maps, and distribution keys to plan execution. Cost driver: cluster uptime (provisioned) or RPU-seconds (Serverless).

**Key references:**
- [Redshift Best Practices](https://docs.aws.amazon.com/redshift/latest/dg/best-practices.html)
- [Redshift System Tables Reference](https://docs.aws.amazon.com/redshift/latest/dg/c_intro_system_tables.html)
- [Redshift Query Planning](https://docs.aws.amazon.com/redshift/latest/dg/c-the-query-plan.html)

---

## 1. Data Modeling & Schema Design

### Schema Pattern

Star schema is the recommended pattern. Fact tables should be large with `DISTKEY` on the join column to co-locate with dimension lookups. Dimension tables are typically small and can use `DISTSTYLE ALL` to replicate to every node.

```sql
-- Fact table: distributed by join key, sorted by most-common filter
CREATE TABLE fact_orders (
    order_id    BIGINT,
    customer_id BIGINT DISTKEY,
    order_date  DATE,
    marketplace_id INT,
    revenue     DECIMAL(18,2)
) SORTKEY (order_date, marketplace_id);

-- Small dimension: replicate to all nodes (eliminates redistribution on join)
CREATE TABLE dim_customer (
    customer_id BIGINT,
    name        VARCHAR(200),
    region      VARCHAR(50)
) DISTSTYLE ALL;
```

- [Table design best practices](https://docs.aws.amazon.com/redshift/latest/dg/c_designing-tables-best-practices.html)

### Aggregate & Rollup Design

Use materialized views with auto-refresh for stable aggregations. `SVV_MV_INFO` shows refresh status and staleness.

```sql
CREATE MATERIALIZED VIEW daily_revenue_mv
AUTO REFRESH YES AS
SELECT
    order_date,
    marketplace_id,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS order_count
FROM fact_orders
GROUP BY 1, 2;

-- Check refresh status
SELECT mv_name, is_stale, last_refresh_time FROM SVV_MV_INFO;
```

- [Materialized views](https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-overview.html)

### Time-Series & Trend Metrics

Sort key on the date column enables zone map block skipping. Window functions (LAG, LEAD) are efficient when data is sorted correctly.

```sql
-- Efficient: sort key on order_date means zone maps skip irrelevant blocks
SELECT
    order_date,
    revenue,
    LAG(revenue, 7) OVER (ORDER BY order_date) AS revenue_7d_ago,
    revenue / NULLIF(LAG(revenue, 7) OVER (ORDER BY order_date), 0) - 1 AS wow_growth
FROM daily_revenue_mv
WHERE order_date >= '2024-01-01';
```

- [Window functions](https://docs.aws.amazon.com/redshift/latest/dg/r_Window_function_synopsis.html)

### Slowly Changing Dimensions (SCD)

Use `MERGE` for Type 2 upserts. Stage new records into a staging table first, then merge.

```sql
MERGE INTO dim_customer USING dim_customer_staging AS src
ON dim_customer.customer_id = src.customer_id
    AND dim_customer.valid_to = '9999-12-31'
WHEN MATCHED AND src.region != dim_customer.region THEN
    UPDATE SET valid_to = CURRENT_DATE - 1
WHEN NOT MATCHED THEN
    INSERT (customer_id, region, valid_from, valid_to)
    VALUES (src.customer_id, src.region, CURRENT_DATE, '9999-12-31');
```

- [MERGE statement](https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html)

### High-Cardinality Dimensions (Bridge Tables)

Use a bridge table with `DISTKEY` matching the fact table join key to avoid redistribution.

```sql
-- Bridge table: distributed on the same key as the fact table
CREATE TABLE bridge_product_tag (
    product_id BIGINT DISTKEY,
    tag_id     BIGINT
);
-- Join: no redistribution because both tables dist on product_id
SELECT f.*, t.tag_name
FROM fact_sales f
JOIN bridge_product_tag b ON f.product_id = b.product_id
JOIN dim_tag t ON b.tag_id = t.tag_id;
```

### Access Pattern Alignment

Query `STL_QUERY` and `SVL_QLOG` to understand actual usage patterns before finalizing schema.

```sql
-- Top tables by scan frequency
SELECT
    TRIM(name) AS table_name,
    COUNT(*) AS scan_count,
    SUM(rows) AS total_rows_scanned
FROM stl_scan
JOIN pg_class ON pg_class.oid = stl_scan.tbl
WHERE starttime >= DATEADD(day, -7, GETDATE())
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;
```

- [SVL_QLOG](https://docs.aws.amazon.com/redshift/latest/dg/r_SVL_QLOG.html)

---

## 2. Physical Storage & Layout

### Storage Format

Internal Redshift tables use a proprietary columnar format. External tables via Redshift Spectrum support Parquet, ORC, and Iceberg on S3.

```sql
-- External schema for S3 data
CREATE EXTERNAL SCHEMA spectrum_schema
FROM DATA CATALOG
DATABASE 'my_glue_db'
IAM_ROLE 'arn:aws:iam::123456789:role/MyRedshiftRole';

-- Query Parquet files on S3 directly
SELECT * FROM spectrum_schema.fact_orders_parquet
WHERE order_date = '2024-01-01';
```

- [Redshift Spectrum supported formats](https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html)

### File Size

For internal tables, Redshift manages file storage automatically. For Spectrum (external S3 tables), compact files to ~128 MB before querying — small files significantly increase scan overhead.

```sql
-- Check blocks used per table (proxy for data layout health)
SELECT
    TRIM(t.name) AS table_name,
    COUNT(b.blocknum) AS block_count,
    SUM(b.mbytes) AS total_mb
FROM stv_blocklist b
JOIN pg_class c ON c.oid = b.tbl
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN svv_table_info t ON t.table_id = b.tbl
GROUP BY 1
ORDER BY 3 DESC;
```

### Encoding & Compression

Run `ANALYZE COMPRESSION` to get per-column encoding recommendations. Avoid encoding the leading sort key — it disables zone maps. Booleans and small tables don't benefit from compression.

```sql
-- Get encoding recommendations
ANALYZE COMPRESSION fact_orders;

-- Apply recommended encodings (example)
ALTER TABLE fact_orders
ALTER COLUMN marketplace_id ENCODE bytedict;

-- Check current encodings
SELECT column_name, encoding
FROM pg_table_def
WHERE tablename = 'fact_orders';
```

- [Redshift compression encodings](https://docs.aws.amazon.com/redshift/latest/dg/t_Compressing_data_on_disk.html)

### Partitioning & Distribution Strategy

Redshift distributes rows to slices based on `DISTKEY`. Choose distribution style carefully:

| Style | When to use |
|---|---|
| `DISTKEY(col)` | Large tables joining on a common key — co-locates matching rows, eliminates shuffle |
| `DISTSTYLE ALL` | Small dimension tables — replicated to every node, always local |
| `DISTSTYLE EVEN` | Tables with no clear join key or heavily skewed keys |
| `DISTSTYLE AUTO` | Let Redshift decide (good starting point) |

```sql
-- Check distribution style and sort key for a table
SELECT
    table_name,
    diststyle,
    sortkey1,
    sortkey1_enc
FROM svv_table_info
WHERE table_name = 'fact_orders';
```

- [Distribution styles](https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_style.html)
- [Sort keys](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html)

### Data Types

Use minimal types. `INT4` vs `INT8` affects sort and hash performance. Avoid `VARCHAR(MAX)` — it forces variable-length storage even when data is short.

```sql
-- Audit oversized column types
SELECT
    tablename,
    columnname,
    type,
    character_maximum_length
FROM pg_table_def
WHERE schemaname = 'public'
  AND type = 'character varying'
  AND character_maximum_length > 500
ORDER BY character_maximum_length DESC;
```

- [Redshift data types](https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html)

### Clustering Structures & Indexes

Redshift uses zone maps (min/max per block) on sort key columns to skip irrelevant blocks. B-tree indexes don't exist.

**Sort key rules:**
- Do not encode the leading sort key (disables zone maps)
- No benefit for tables under 1 MB
- First 8 characters of the sort key are compared — if all values share the same prefix, zone maps don't help
- Compound sort key: effective for queries filtering on leading columns in order
- Interleaved sort key: effective when queries filter on different combinations of columns

```sql
-- Check sort key skew (percentage of rows in sorted order)
SELECT
    TRIM(name) AS table_name,
    unsorted AS pct_unsorted,
    stats_off AS stats_freshness
FROM svv_table_info
WHERE unsorted > 20  -- flag tables more than 20% unsorted
ORDER BY unsorted DESC;
```

- [Redshift sort keys best practices](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html)

### Table Statistics

`ANALYZE` updates statistics. Auto-analyze runs in the background but may lag after large data loads. Run manually after bulk inserts.

```sql
-- Update statistics for a specific table
ANALYZE fact_orders;

-- Check statistics freshness for all tables
SELECT
    table_name,
    stats_off,
    last_analyze
FROM svv_table_info
WHERE stats_off > 10  -- flag tables with stale stats
ORDER BY stats_off DESC;
```

- [ANALYZE command](https://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE.html)

### Metadata (Open Table Formats)

Redshift Spectrum reads Iceberg and Delta metadata from the Glue Data Catalog. Redshift does not manage open table format metadata — maintenance (snapshot expiry, compaction) must be done via Athena or Glue ETL.

```sql
-- Query an Iceberg table through Spectrum
CREATE EXTERNAL TABLE spectrum_schema.orders_iceberg
STORED AS PARQUET
LOCATION 's3://my-bucket/orders/';
```

- [Redshift Spectrum with Iceberg](https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html)

---

## 3. Data Ingestion

### Load Pattern (Bulk / Incremental / CDC)

`COPY` is the highest-throughput bulk load mechanism — uses parallel S3 reads across all slices. Always prefer `COPY` over `INSERT ... SELECT` for large loads.

```sql
-- Bulk load from S3 (recommended for large datasets)
COPY fact_orders
FROM 's3://my-bucket/orders/2024/01/'
IAM_ROLE 'arn:aws:iam::123456789:role/MyRedshiftRole'
FORMAT AS PARQUET;

-- Incremental upsert (CDC pattern)
-- Step 1: Load new/changed records into staging
COPY staging_orders FROM 's3://...' IAM_ROLE '...' FORMAT AS PARQUET;

-- Step 2: Delete changed records from target
DELETE FROM fact_orders USING staging_orders s
WHERE fact_orders.order_id = s.order_id;

-- Step 3: Insert all staging records (new + updated)
INSERT INTO fact_orders SELECT * FROM staging_orders;
```

- [COPY command](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html)
- [MERGE command](https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html)

### Source-Specific Considerations

Handle encoding issues and semi-structured sources with COPY parameters.

```sql
-- Handle encoding issues
COPY fact_orders FROM 's3://...'
IAM_ROLE '...'
CSV
ACCEPTINVCHARS '?'         -- replace invalid UTF-8 chars
IGNOREBLANKLINES
TRUNCATECOLUMNS;           -- truncate values exceeding column width

-- Semi-structured JSON with JSONPaths
COPY fact_events FROM 's3://...'
IAM_ROLE '...'
JSON 's3://my-bucket/jsonpaths.json';
```

- [COPY parameters](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY-parameters.html)

---

## 4. Query & Compute Execution

### Data Reduction

Zone maps on sort key columns enable block-level skipping. Functions on sort key columns disable zone maps.

```sql
-- Good: predicate on sort key column — zone maps active
SELECT * FROM fact_orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'
  AND marketplace_id = 1;

-- Bad: function on filter column — zone maps disabled
-- YEAR(order_date) = 2024 → Redshift cannot use zone maps on this
SELECT * FROM fact_orders
WHERE YEAR(order_date) = 2024;

-- Check EXPLAIN for scan details
EXPLAIN SELECT * FROM fact_orders WHERE order_date = '2024-01-15';
```

- [Query planning and execution](https://docs.aws.amazon.com/redshift/latest/dg/c-the-query-plan.html)

### Join Strategy

EXPLAIN output shows redistribution type:

| Label in EXPLAIN | Meaning |
|---|---|
| `DS_DIST_NONE` | No redistribution — tables already co-located on dist key |
| `DS_DIST_ALL` | Small table broadcast to all slices |
| `DS_DIST_INNER` | Inner table redistributed |
| `DS_DIST_BOTH` | Both tables redistributed — most expensive, indicates missing dist key |

```sql
-- Check join redistribution in execution plan
EXPLAIN
SELECT f.*, d.region
FROM fact_orders f
JOIN dim_customer d ON f.customer_id = d.customer_id
WHERE f.order_date = '2024-01-15';
-- Look for DS_DIST_NONE or DS_DIST_ALL — good
-- DS_DIST_BOTH means add/change DISTKEY
```

### Data Movement & Shuffle

Design distribution keys to eliminate `DS_DIST_BOTH`. When two large tables join, both should have the same `DISTKEY` on the join column.

```sql
-- Check for redistribution events in query history
SELECT
    q.query,
    q.querytxt,
    s.is_diskbased,
    s.workmem
FROM stl_query q
JOIN stl_stream_segs s ON q.query = s.query
WHERE q.starttime >= DATEADD(hour, -1, GETDATE())
ORDER BY s.workmem DESC;
```

- [Redshift redistribution](https://docs.aws.amazon.com/redshift/latest/dg/c-the-query-plan.html)

### Data Skew

Detect and address data skew using `STL_ALERT_EVENT_LOG`. For heavily skewed tables with no clear dist key, use `DISTSTYLE EVEN`.

```sql
-- Find skew alerts
SELECT
    trim(s.perm_table_name) AS table_name,
    sum(abs(datediff(milliseconds, s.starttime, s.endtime))) AS skew_ms,
    trim(a.event) AS alert_event
FROM stl_alert_event_log a
JOIN stl_scan s ON s.query = a.query AND s.slice = a.slice
WHERE a.event LIKE '%skew%'
  AND a.starttime >= DATEADD(day, -1, GETDATE())
GROUP BY 1, 3
ORDER BY 2 DESC;
```

- [Table distribution guide](https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_style.html)

### Resource Allocation (Compute / Memory)

WLM (Workload Management) controls memory allocation per query class. Queues with more memory slots can run more concurrent queries but each gets less memory.

```sql
-- Check WLM queue configuration
SELECT service_class, num_query_tasks, query_working_mem
FROM stv_wlm_service_class_config
WHERE service_class > 4  -- 5+ are user-defined queues
ORDER BY service_class;

-- Check for recent memory spill events
SELECT query, step, rows, workmem, label, is_diskbased
FROM svl_query_summary
WHERE is_diskbased = 't'
  AND starttime >= DATEADD(hour, -6, GETDATE())
ORDER BY workmem DESC;
```

- [Workload Management](https://docs.aws.amazon.com/redshift/latest/dg/c_workload_mngmt_classification.html)

### Approximate Computation

`APPROXIMATE COUNT(DISTINCT col)` uses HyperLogLog internally with ~2% error. 10–100x faster than exact COUNT DISTINCT on high-cardinality columns.

```sql
-- Exact (expensive for high-cardinality)
SELECT COUNT(DISTINCT customer_id) FROM fact_orders;

-- Approximate (fast, ~2% error — suitable for dashboards)
SELECT APPROXIMATE COUNT(DISTINCT customer_id) FROM fact_orders;
```

- [APPROXIMATE COUNT DISTINCT](https://docs.aws.amazon.com/redshift/latest/dg/r_APPROXIMATE_COUNT_DISTINCT.html)

### SQL Construct Choices

```sql
-- CTEs: NOT materialized by default — inlined by optimizer
-- Use temp tables to force materialization
WITH expensive_cte AS (
    SELECT customer_id, SUM(revenue) AS total
    FROM fact_orders GROUP BY 1
)
SELECT * FROM expensive_cte WHERE total > 1000;
-- If expensive_cte is referenced multiple times, optimizer may recompute it
-- Solution: CREATE TEMP TABLE expensive_base AS (SELECT ...)

-- UNION vs UNION ALL
-- UNION: deduplicates (sorts/hashes full result) — use only when duplicates are possible
-- UNION ALL: no dedup — always prefer when duplicates are impossible
SELECT customer_id FROM fact_orders_2023
UNION ALL  -- not UNION
SELECT customer_id FROM fact_orders_2024;
```

- [WITH clause (CTEs)](https://docs.aws.amazon.com/redshift/latest/dg/r_WITH_clause.html)

### Execution Plan Analysis

```sql
-- Static EXPLAIN (before execution)
EXPLAIN
SELECT f.customer_id, SUM(f.revenue)
FROM fact_orders f
JOIN dim_customer d ON f.customer_id = d.customer_id
WHERE f.order_date >= '2024-01-01'
GROUP BY 1;

-- Post-execution: actual execution steps stored in STL_EXPLAIN
SELECT plannode, info
FROM stl_explain
WHERE query = <query_id>
ORDER BY nodeid;

-- Compare estimated vs actual rows for a recent query
SELECT
    query,
    step,
    rows AS actual_rows,
    rows_pre_filter AS estimated_rows,
    ROUND(rows::FLOAT / NULLIF(rows_pre_filter, 0), 2) AS actual_vs_estimate_ratio
FROM svl_query_summary
WHERE query = <query_id>
ORDER BY step;
```

- [EXPLAIN command](https://docs.aws.amazon.com/redshift/latest/dg/r_EXPLAIN.html)
- [STL_EXPLAIN](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_EXPLAIN.html)

---

## 5. Precomputation & Reuse

### Materialized Views

```sql
-- Create with auto-refresh
CREATE MATERIALIZED VIEW mv_daily_sales
AUTO REFRESH YES AS
SELECT
    TRUNC(order_date) AS sale_date,
    marketplace_id,
    SUM(revenue) AS total_revenue
FROM fact_orders
GROUP BY 1, 2;

-- Check refresh status
SELECT mv_name, is_stale, last_refresh_time, state
FROM svv_mv_info;

-- Manually refresh if needed
REFRESH MATERIALIZED VIEW mv_daily_sales;
```

- [Materialized views overview](https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-overview.html)

### Engine-Level Caching

Result cache is enabled by default. Identical queries (same text, same user, data unchanged) return instantly from cache.

```sql
-- Enable/disable result cache per session
SET enable_result_cache_for_session = ON;

-- Check if last query used cache
SELECT query, from_sp_cache
FROM stl_query
WHERE query = pg_last_query_id();
```

- [Result cache](https://docs.aws.amazon.com/redshift/latest/dg/r_enable_result_cache_for_session.html)

### Pre-aggregated Tables

```sql
-- Schedule a query to refresh a pre-aggregated table
-- Using Redshift scheduled queries (via console or API)
CREATE OR REPLACE PROCEDURE refresh_weekly_agg()
AS $$
BEGIN
    DELETE FROM agg_weekly_sales WHERE week_start >= DATEADD(week, -2, GETDATE());
    INSERT INTO agg_weekly_sales
    SELECT
        DATE_TRUNC('week', order_date) AS week_start,
        marketplace_id,
        SUM(revenue) AS revenue
    FROM fact_orders
    WHERE order_date >= DATEADD(week, -2, GETDATE())
    GROUP BY 1, 2;
END;
$$ LANGUAGE plpgsql;
```

- [Scheduled queries](https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-schedule-query.html)

---

## 6. Operations & Lifecycle

### Table Maintenance (Vacuum / Compaction / Stats)

```sql
-- Vacuum reclaims space from deleted rows and re-sorts unsorted data
VACUUM fact_orders;

-- Vacuum options:
-- FULL: sort + reclaim space (most thorough, most I/O)
-- SORT ONLY: re-sort unsorted rows (no space reclaim)
-- DELETE ONLY: reclaim space only (no sort)
-- REINDEX: for interleaved sort keys

-- Check tables needing vacuum
SELECT
    TRIM(name) AS table_name,
    rows,
    unsorted AS pct_unsorted,
    vacuum_sort_benefit
FROM svv_table_info
WHERE vacuum_sort_benefit > 50  -- high benefit from vacuuming
ORDER BY vacuum_sort_benefit DESC;

-- Auto-vacuum runs in background; manual vacuum forces immediate execution
```

- [VACUUM command](https://docs.aws.amazon.com/redshift/latest/dg/r_VACUUM_command.html)

### Data Retention & Archival

```sql
-- Unload old data to S3 (cheap storage) before deleting from Redshift
UNLOAD ('SELECT * FROM fact_orders WHERE order_date < ''2022-01-01''')
TO 's3://my-archive-bucket/orders/2021/'
IAM_ROLE 'arn:aws:iam::123456789:role/MyRedshiftRole'
FORMAT AS PARQUET;

-- Then delete from Redshift
DELETE FROM fact_orders WHERE order_date < '2022-01-01';
VACUUM DELETE ONLY fact_orders;  -- reclaim space from deletes
```

- [UNLOAD command](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html)

### Database-Level Operations

```sql
-- Create manual snapshot before major operations (via AWS CLI)
-- aws redshift create-cluster-snapshot --cluster-identifier my-cluster --snapshot-identifier pre-migration-2024

-- Restore from snapshot
-- aws redshift restore-from-cluster-snapshot --cluster-identifier new-cluster --snapshot-identifier pre-migration-2024

-- Validate row counts post-load
SELECT COUNT(*) FROM fact_orders;
SELECT COUNT(*) FROM staging_orders;
```

- [Working with snapshots](https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-snapshots.html)

### Cluster & Infrastructure Operations

```sql
-- Check cluster utilization
SELECT
    instance_type,
    num_nodes,
    estimated_base_capacity_size_gb
FROM stv_node_storage_capacity;

-- Monitor query queue depth
SELECT
    service_class,
    num_queued_queries,
    num_executing_queries
FROM stv_wlm_service_class_state
WHERE service_class > 4;
```

- [Cluster management](https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-clusters.html)
- [Redshift Serverless](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-overview.html)

---

## 7. Observability

### Resource & Query Metrics

Key system tables for query analysis:

| Table | Purpose |
|---|---|
| `SVL_QUERY_METRICS` | Per-query resource consumption (CPU, memory, I/O) |
| `STL_QUERY` | Query history with execution time |
| `SVL_QLOG` | Lightweight query log |
| `STL_ALERT_EVENT_LOG` | Optimizer warnings (skew, missing stats, spills) |
| `STV_EXEC_STATE` | Live query execution state |

```sql
-- Find slow queries in the last hour
SELECT
    q.query,
    TRIM(q.querytxt) AS query_text,
    DATEDIFF(seconds, q.starttime, q.endtime) AS duration_sec,
    q.aborted
FROM stl_query q
WHERE q.starttime >= DATEADD(hour, -1, GETDATE())
  AND q.userid > 1
ORDER BY duration_sec DESC
LIMIT 20;

-- Check for alerts (skew, spills, missing stats)
SELECT
    query,
    trim(event) AS alert_event,
    trim(solution) AS suggested_solution
FROM stl_alert_event_log
WHERE starttime >= DATEADD(hour, -1, GETDATE())
ORDER BY query DESC;
```

- [Redshift system tables](https://docs.aws.amazon.com/redshift/latest/dg/c_intro_system_tables.html)

### Adoption & Throughput Metrics

```sql
-- Query volume by user over last 7 days
SELECT
    TRIM(u.usename) AS username,
    COUNT(*) AS query_count,
    AVG(DATEDIFF(seconds, q.starttime, q.endtime)) AS avg_duration_sec,
    MAX(DATEDIFF(seconds, q.starttime, q.endtime)) AS max_duration_sec
FROM stl_query q
JOIN pg_user u ON q.userid = u.usesysid
WHERE q.starttime >= DATEADD(day, -7, GETDATE())
  AND q.userid > 1
GROUP BY 1
ORDER BY 2 DESC;

-- WLM queue wait times (signal of under-resourcing)
SELECT
    service_class,
    COUNT(*) AS queries,
    AVG(total_queue_time) / 1000000 AS avg_queue_sec,
    MAX(total_queue_time) / 1000000 AS max_queue_sec
FROM stl_wlm_query
WHERE service_class > 4
  AND queue_start_time >= DATEADD(day, -1, GETDATE())
GROUP BY 1;
```

- [WLM monitoring](https://docs.aws.amazon.com/redshift/latest/dg/cm-c-wlm-query-monitoring-rules.html)

### Execution Plan Inspection

See [Data Reduction](#data-reduction) and [Execution Plan Analysis](#execution-plan-analysis) above. Key commands: `EXPLAIN`, `STL_EXPLAIN`, `SVL_QUERY_SUMMARY`.

### Alerts & Automation

Use **Query Monitoring Rules (QMR)** in WLM to automatically cancel or hop runaway queries.

```sql
-- Example QMR rule (configured via console or API):
-- Rule: if query_execution_time > 600 seconds AND scan_row_count > 1B rows
-- Action: abort query

-- Check QMR rule actions in history
SELECT
    query,
    trim(action) AS action_taken,
    trim(rule_name) AS rule_name,
    trim(trigger_condition) AS trigger
FROM stl_wlm_rule_action
WHERE recordtime >= DATEADD(day, -1, GETDATE())
ORDER BY recordtime DESC;
```

- [Query Monitoring Rules](https://docs.aws.amazon.com/redshift/latest/dg/cm-c-wlm-query-monitoring-rules.html)
- [CloudWatch metrics for Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/metrics-listing.html)
