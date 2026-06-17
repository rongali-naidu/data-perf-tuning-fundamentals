# Athena Implementation Guide

> **Engine profile:** Serverless interactive query service based on Presto/Trino. No cluster to manage. Queries run on-demand against data in S3. Cost driver: **bytes scanned per query** — every optimization that reduces scan size directly reduces cost. Supports Parquet, ORC, JSON, CSV, Iceberg, Delta (read), and Hudi.

**Key references:**
- [Athena performance tuning](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning.html)
- [Athena best practices](https://docs.aws.amazon.com/athena/latest/ug/best-practices-athena.html)
- [Athena SQL reference](https://docs.aws.amazon.com/athena/latest/ug/select.html)

---

## 1. Data Modeling & Schema Design

### Schema Pattern

Star schema with Parquet + partitioning is the recommended pattern. Since Athena charges per bytes scanned, every join that can be eliminated (via denormalization or OBT) saves money. Schema design is directly a cost optimization.

```sql
-- Fact table: partitioned by date + marketplace for efficient pruning
CREATE EXTERNAL TABLE fact_orders (
    order_id        BIGINT,
    customer_id     BIGINT,
    revenue         DECIMAL(18,2),
    product_id      BIGINT
)
PARTITIONED BY (order_date STRING, marketplace_id INT)
STORED AS PARQUET
LOCATION 's3://bucket/fact_orders/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- Add partitions (or use partition projection to avoid Glue Catalog overhead)
MSCK REPAIR TABLE fact_orders;
```

### Aggregate & Rollup Design

Use CTAS to create pre-aggregated tables. Athena has no native materialized views — refresh via a scheduled Glue job or EventBridge rule that runs a CTAS.

```sql
-- Create aggregated table via CTAS
CREATE TABLE agg_daily_revenue
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    partitioned_by = ARRAY['order_date'],
    location = 's3://bucket/agg/daily_revenue/'
)
AS
SELECT
    order_date,
    marketplace_id,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS order_count
FROM fact_orders
GROUP BY 1, 2;
```

- [Athena CTAS](https://docs.aws.amazon.com/athena/latest/ug/ctas.html)

### Time-Series & Trend Metrics

Window functions are fully supported. Partition by date to ensure time-range queries scan only relevant data.

```sql
-- WoW growth: window function on pre-aggregated daily table (cheap)
SELECT
    order_date,
    marketplace_id,
    total_revenue,
    LAG(total_revenue, 7) OVER (
        PARTITION BY marketplace_id
        ORDER BY order_date
    ) AS revenue_7d_ago,
    total_revenue / NULLIF(LAG(total_revenue, 7) OVER (
        PARTITION BY marketplace_id ORDER BY order_date
    ), 0) - 1 AS wow_growth
FROM agg_daily_revenue
WHERE order_date >= '2024-01-01';
-- Query against agg_daily_revenue (small), not fact_orders (large)
```

### Slowly Changing Dimensions (SCD)

Use Iceberg `MERGE INTO` for Type 2 SCD. Athena natively supports Iceberg with ACID operations.

```sql
-- Iceberg Type 2 SCD merge
MERGE INTO iceberg_dim_customer AS target
USING staging_customer AS src
ON target.customer_id = src.customer_id
    AND target.valid_to = date '9999-12-31'
WHEN MATCHED AND src.region != target.region
    THEN UPDATE SET valid_to = current_date - interval '1' day
WHEN NOT MATCHED
    THEN INSERT (customer_id, region, valid_from, valid_to)
         VALUES (src.customer_id, src.region, current_date, date '9999-12-31');
```

- [Iceberg DML on Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-updating-iceberg-table-data.html)

### High-Cardinality Dimensions (Bridge Tables)

`UNNEST` is available for array expansion in ad-hoc queries. For repeated queries, pre-explode into a bridge table via a CTAS job.

```sql
-- Ad-hoc: UNNEST for array columns (scans full table — expensive)
SELECT p.product_id, t.tag
FROM raw_products p
CROSS JOIN UNNEST(p.tags) AS t(tag)
WHERE p.category = 'electronics';

-- Better: pre-exploded bridge table (query the small bridge table)
CREATE TABLE bridge_product_tag
WITH (format = 'PARQUET', location = 's3://bucket/bridge/product_tag/')
AS
SELECT product_id, tag
FROM raw_products
CROSS JOIN UNNEST(tags) AS t(tag);

-- Queries join the bridge table directly
SELECT f.*, b.tag
FROM fact_sales f
JOIN bridge_product_tag b ON f.product_id = b.product_id
WHERE b.tag = 'wireless';
```

### Access Pattern Alignment

Use Athena query history and CloudWatch metrics to identify frequent filter columns. Design partitions around those columns.

```sql
-- Check workgroup query history (via Athena console or API)
-- Filter by: BytesScanned DESC → find most expensive queries
-- Identify common WHERE clauses → these should be partition columns
```

---

## 2. Physical Storage & Layout

### Storage Format

Parquet/ORC are strongly recommended — Athena charges per bytes scanned, and columnar formats typically reduce scanned bytes by 70–90% vs CSV/JSON for analytical queries.

```sql
-- Convert existing CSV/JSON table to Parquet via CTAS
CREATE TABLE fact_orders_parquet
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    partitioned_by = ARRAY['order_date'],
    location = 's3://bucket/fact_orders_parquet/'
)
AS SELECT * FROM fact_orders_csv;

-- Iceberg table (ACID, time travel, schema evolution)
CREATE TABLE iceberg_orders (
    order_id    BIGINT,
    customer_id BIGINT,
    revenue     DOUBLE
)
PARTITIONED BY (order_date DATE)
LOCATION 's3://bucket/iceberg/orders/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'PARQUET');
```

- [Columnar storage best practices](https://docs.aws.amazon.com/athena/latest/ug/columnar-storage.html)

### File Size

Target ~128 MB per file. Small files cause excessive S3 LIST and HEAD API calls, adding latency and cost.

```sql
-- Compact small files using Iceberg OPTIMIZE
OPTIMIZE iceberg_orders REWRITE DATA USING BIN_PACK
WHERE order_date >= '2024-01-01';

-- Check approximate file count per partition via S3 CLI (external to Athena)
-- aws s3 ls s3://bucket/fact_orders/order_date=2024-01-15/ | wc -l
-- Target: 1-10 files per partition for typical workloads
```

- [Iceberg data optimization on Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-data-optimization.html)

### Encoding & Compression

```sql
-- Parquet with Snappy (fast, good ratio) — recommended default
CREATE TABLE ... WITH (format = 'PARQUET', parquet_compression = 'SNAPPY', ...)

-- Zstd (better ratio, similar speed) — good for cold/archival tables
CREATE TABLE ... WITH (format = 'PARQUET', parquet_compression = 'ZSTD', ...)

-- ORC with Zlib
CREATE TABLE ... WITH (format = 'ORC', orc_compression = 'ZLIB', ...)

-- Verify impact: BytesScanned in query result metadata
-- Run same query on compressed vs uncompressed and compare
```

- [Athena compression formats](https://docs.aws.amazon.com/athena/latest/ug/compression-formats.html)

### Partitioning & Distribution Strategy

Partitioning is the single highest-impact optimization for Athena — it directly reduces bytes scanned and thus cost.

```sql
-- Standard Hive-style partitioning
CREATE EXTERNAL TABLE fact_orders (
    order_id BIGINT,
    revenue  DOUBLE
)
PARTITIONED BY (order_date STRING, marketplace_id INT)
STORED AS PARQUET
LOCATION 's3://bucket/fact_orders/';

-- Partition projection: avoids Glue Catalog overhead for high-cardinality partitions
-- Define partition range directly in table properties
CREATE EXTERNAL TABLE fact_orders_projected (
    order_id BIGINT,
    revenue  DOUBLE
)
PARTITIONED BY (order_date STRING, marketplace_id INT)
STORED AS PARQUET
LOCATION 's3://bucket/fact_orders/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.order_date.type' = 'date',
    'projection.order_date.range' = '2020-01-01,NOW',
    'projection.order_date.format' = 'yyyy-MM-dd',
    'projection.marketplace_id.type' = 'integer',
    'projection.marketplace_id.range' = '1,20',
    'storage.location.template' = 's3://bucket/fact_orders/order_date=${order_date}/marketplace_id=${marketplace_id}/'
);
```

- [Athena partitioning best practices](https://docs.aws.amazon.com/athena/latest/ug/partitions.html)
- [Partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)

### Data Types

Use `DATE` not `STRING` for date columns — string comparisons don't match partition pruning logic and can cause full scans.

```sql
-- Bad: date stored as STRING — partition pruning may not work correctly
WHERE order_date > '2024-01-01'  -- string comparison, not date comparison

-- Good: use DATE type
WHERE order_date > DATE '2024-01-01'

-- Audit column types in Glue Catalog
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'my_db'
  AND table_name = 'fact_orders'
ORDER BY ordinal_position;
```

- [Athena data types](https://docs.aws.amazon.com/athena/latest/ug/data-types.html)

### Clustering Structures & Indexes

No B-tree indexes. Iceberg provides Z-ordering and bloom filters via `OPTIMIZE`.

```sql
-- Iceberg Z-order: cluster data for multi-dimensional range queries
OPTIMIZE iceberg_orders
REWRITE DATA USING BIN_PACK
WHERE order_date >= DATE '2024-01-01'
ORDER BY order_date, marketplace_id;

-- After Z-ordering: queries with AND filters on both columns skip far more files

-- Check Iceberg table properties for sorting config
SELECT *
FROM TABLE(system.describe_table_extended(schema=>'my_db', table_name=>'iceberg_orders'));
```

- [Iceberg optimization on Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-data-optimization.html)

### Table Statistics

Statistics are managed by the Glue Data Catalog. Iceberg tables maintain built-in column-level statistics automatically.

```sql
-- For Hive-style tables: update Glue Catalog statistics
-- (done via Glue Crawler or manual metadata update)

-- For Iceberg: statistics are automatically maintained
-- Check column stats:
SELECT *
FROM "my_db"."iceberg_orders$files"
LIMIT 10;

-- Check partition stats
SELECT *
FROM "my_db"."iceberg_orders$partitions";
```

- [Athena and Glue statistics](https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html)

### Metadata (Open Table Formats)

Iceberg is natively supported in Athena. Metadata files are stored in S3; the Glue Catalog tracks the current metadata pointer.

```sql
-- Query Iceberg metadata tables
SELECT * FROM "my_db"."iceberg_orders$snapshots" ORDER BY committed_at DESC LIMIT 5;
SELECT * FROM "my_db"."iceberg_orders$history";
SELECT * FROM "my_db"."iceberg_orders$files" LIMIT 10;
SELECT * FROM "my_db"."iceberg_orders$manifests";

-- Time travel
SELECT * FROM iceberg_orders FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 00:00:00';
SELECT * FROM iceberg_orders FOR VERSION AS OF 12345;

-- Expire snapshots to control metadata bloat
VACUUM iceberg_orders;
```

- [Querying Iceberg table metadata on Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-table-metadata.html)

---

## 3. Data Ingestion

### Load Pattern (Bulk / Incremental / CDC)

Athena is primarily a query engine — it reads data from S3. Data is written by Glue ETL, Spark jobs, or CTAS statements.

```sql
-- Bulk load via CTAS (reads from source, writes to S3)
CREATE TABLE fact_orders_2024
WITH (
    format = 'PARQUET',
    location = 's3://bucket/fact_orders_2024/'
)
AS SELECT * FROM source_table WHERE YEAR(order_date) = 2024;

-- Incremental append to Iceberg
INSERT INTO iceberg_orders
SELECT * FROM staging_orders
WHERE order_date = CURRENT_DATE;

-- CDC via Iceberg MERGE
MERGE INTO iceberg_orders AS target
USING staging_orders AS src
ON target.order_id = src.order_id
WHEN MATCHED AND src.status = 'DELETED' THEN DELETE
WHEN MATCHED THEN UPDATE SET revenue = src.revenue
WHEN NOT MATCHED THEN INSERT VALUES (src.order_id, src.customer_id, src.revenue, src.order_date);
```

- [Iceberg MERGE on Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-updating-iceberg-table-data.html)

### Source-Specific Considerations

```sql
-- Schema-on-read: Athena reads schema from Glue Catalog at query time
-- Adding a column to source files doesn't break existing queries
-- New column returns NULL for old files until explicitly populated

-- Iceberg schema evolution: safe column adds, renames, drops
ALTER TABLE iceberg_orders ADD COLUMNS (region VARCHAR);
ALTER TABLE iceberg_orders RENAME COLUMN region TO marketplace_region;
ALTER TABLE iceberg_orders DROP COLUMN old_column;

-- Detect schema drift: compare Glue Catalog schema vs actual Parquet file schema
-- Use Glue Crawler on a schedule to update catalog
```

- [Iceberg schema evolution on Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-schema-evolution.html)

---

## 4. Query & Compute Execution

### Data Reduction

Athena charges per bytes scanned — data reduction is both a performance and cost optimization.

```sql
-- Good: partition filter applied — only scans matching partitions
SELECT SUM(revenue)
FROM fact_orders
WHERE order_date >= '2024-01-01'   -- partition column: scans only 2024+ partitions
  AND marketplace_id = 1;           -- partition column: further reduces scan

-- Bad: function on partition column disables partition pruning
SELECT SUM(revenue)
FROM fact_orders
WHERE YEAR(order_date) = 2024;    -- full table scan: Athena cannot prune on YEAR(col)

-- Bad: leading wildcard disables any file skipping
WHERE product_name LIKE '%wireless%'  -- full scan

-- Verify bytes scanned in query result:
-- Athena console shows "Data scanned: X MB" for each query
-- Target: scanned bytes << total table size
```

### Join Strategy

Athena (Presto/Trino) uses cost-based optimization to select join strategy. Override with session properties or hints.

```sql
-- Force broadcast join via session property
SET join_distribution_type = 'BROADCAST';

-- Force broadcast for a specific table in query (hint)
SELECT /*+ BROADCAST(d) */ f.*, d.region
FROM fact_orders f
JOIN dim_customer d ON f.customer_id = d.customer_id
WHERE f.order_date >= '2024-01-01';

-- Check join strategy in EXPLAIN output
EXPLAIN
SELECT f.*, d.region
FROM fact_orders f
JOIN dim_customer d ON f.customer_id = d.customer_id;
-- Look for: "- InnerJoin[...] => [...]  Distribution: REPLICATED" (broadcast)
-- vs "Distribution: PARTITIONED" (shuffle)
```

- [Athena performance tuning](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning.html)

### Data Movement & Shuffle

Athena manages shuffle internally — no explicit shuffle configuration. Optimize through table design and query structure.

```sql
-- Reduce shuffle by filtering before joining
-- Good: filter early, join on smaller intermediate result
WITH recent_orders AS (
    SELECT customer_id, SUM(revenue) AS revenue
    FROM fact_orders
    WHERE order_date >= '2024-01-01'  -- filter first
    GROUP BY customer_id
)
SELECT r.*, d.region
FROM recent_orders r
JOIN dim_customer d ON r.customer_id = d.customer_id;

-- Avoid cross joins (Cartesian product)
-- An accidental missing join condition creates a cross join on Athena
```

### Data Skew

```sql
-- Identify skew: check if partitions have very unequal sizes
-- aws s3 ls --recursive s3://bucket/fact_orders/ | awk '{print $3}' | sort -n

-- Address skew by adding a secondary partition column to distribute load
-- If marketplace_id=1 has 80% of data, add sub-partition:
-- PARTITIONED BY (order_date STRING, marketplace_id INT, shard_id INT)
-- And distribute records using hash(order_id) % 10 as shard_id

-- For skewed joins: filter the skewed values and process separately
-- Join non-null customer_id rows normally
SELECT f.*, d.region
FROM fact_orders f
JOIN dim_customer d ON f.customer_id = d.customer_id
WHERE f.customer_id IS NOT NULL

UNION ALL

-- Handle null customer_id separately (no join)
SELECT f.*, NULL AS region
FROM fact_orders f
WHERE f.customer_id IS NULL;
```

### Resource Allocation (Compute / Memory)

Athena is serverless — no direct memory configuration. Break up large queries if they hit memory limits.

```sql
-- Split large queries using CTAS to write intermediate results
-- Step 1: filter and aggregate (cheaper intermediate)
CREATE TABLE temp_filtered
WITH (format = 'PARQUET', location = 's3://bucket/temp/filtered/')
AS
SELECT customer_id, SUM(revenue) AS total
FROM fact_orders
WHERE order_date BETWEEN '2023-01-01' AND '2024-01-01'
GROUP BY customer_id;

-- Step 2: join on smaller intermediate result
SELECT t.*, d.region
FROM temp_filtered t
JOIN dim_customer d ON t.customer_id = d.customer_id;

-- For recurring complex queries, split into materialized steps via Glue ETL
```

- [Athena performance tuning](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning.html)

### Approximate Computation

Athena (Presto/Trino) natively supports HyperLogLog via `approx_distinct`.

```sql
-- Exact COUNT DISTINCT (expensive for high-cardinality)
SELECT COUNT(DISTINCT customer_id) FROM fact_orders;

-- Approximate (HLL, ~2% error, much faster and cheaper)
SELECT approx_distinct(customer_id) FROM fact_orders;

-- With custom error bound
SELECT approx_distinct(customer_id, 0.005) FROM fact_orders;  -- 0.5% error

-- Percentiles (approximate)
SELECT approx_percentile(revenue, 0.95) AS p95_revenue FROM fact_orders;
SELECT approx_percentile(revenue, ARRAY[0.5, 0.75, 0.95]) AS percentiles FROM fact_orders;
```

- [Presto aggregate functions (approx_distinct)](https://prestodb.io/docs/current/functions/aggregate.html#approx_distinct)

### SQL Construct Choices

```sql
-- CTEs: Presto/Trino inlines CTEs by default
-- If a CTE is expensive and used multiple times, write to a temp table via CTAS

-- UNION ALL vs UNION
SELECT customer_id FROM orders_2023
UNION ALL  -- preferred: no dedup
SELECT customer_id FROM orders_2024;

-- UNION (adds dedup step — only use when duplicates are possible)
SELECT customer_id FROM orders_2023
UNION
SELECT customer_id FROM orders_2024;

-- IN vs EXISTS: for semi-joins
-- EXISTS is often more efficient when the subquery can short-circuit
SELECT * FROM fact_orders f
WHERE EXISTS (
    SELECT 1 FROM high_value_customers c WHERE c.customer_id = f.customer_id
);

-- Avoid implicit type coercions in join conditions
-- Bad: joins a BIGINT to a VARCHAR — forces cast on every row
WHERE f.customer_id = d.customer_id_string  -- implicit cast disables optimization
```

- [Athena SQL SELECT reference](https://docs.aws.amazon.com/athena/latest/ug/select.html)

### Execution Plan Analysis

```sql
-- Static EXPLAIN
EXPLAIN
SELECT f.customer_id, SUM(f.revenue)
FROM fact_orders f
JOIN dim_customer d ON f.customer_id = d.customer_id
WHERE f.order_date >= '2024-01-01'
GROUP BY 1;
-- Look for: Fragment, Distribution (BROADCAST vs PARTITIONED), join type

-- EXPLAIN ANALYZE: runs the query and shows actual execution stats
EXPLAIN ANALYZE
SELECT COUNT(*) FROM fact_orders WHERE order_date = '2024-01-15';
-- Shows: actual rows processed, CPU time, wall time per operator
```

- [Athena EXPLAIN statement](https://docs.aws.amazon.com/athena/latest/ug/athena-explain-statement.html)

---

## 5. Precomputation & Reuse

### Materialized Views

Athena has no native materialized views. Use CTAS as equivalent. Schedule refresh via EventBridge + Lambda or Glue workflow.

```sql
-- Create "materialized view" via CTAS
CREATE TABLE mv_daily_revenue
WITH (
    format = 'PARQUET',
    location = 's3://bucket/mv/daily_revenue/',
    partitioned_by = ARRAY['order_date']
)
AS
SELECT
    order_date,
    marketplace_id,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS order_count
FROM fact_orders
GROUP BY 1, 2;

-- Refresh pattern: DROP + recreate (full refresh)
DROP TABLE IF EXISTS mv_daily_revenue;
-- Then recreate with CTAS

-- Incremental refresh for Iceberg: INSERT INTO + DELETE
DELETE FROM iceberg_mv WHERE order_date >= CURRENT_DATE - interval '7' day;
INSERT INTO iceberg_mv
SELECT order_date, marketplace_id, SUM(revenue) AS total_revenue
FROM fact_orders
WHERE order_date >= CURRENT_DATE - interval '7' day
GROUP BY 1, 2;
```

- [Athena CTAS](https://docs.aws.amazon.com/athena/latest/ug/ctas.html)

### Engine-Level Caching

Athena has no result cache — every query is freshly executed. For repeated dashboard queries, options are:
1. Use Redshift Serverless as a caching layer (query Athena once, cache in Redshift)
2. Pre-aggregate into small tables so repeated queries are cheap anyway
3. Use a BI tool with its own result cache

### Pre-aggregated Tables

```sql
-- Daily aggregation (run nightly via EventBridge → Athena named query API)
CREATE TABLE agg_daily
WITH (format = 'PARQUET', location = 's3://bucket/agg/daily/')
AS
SELECT
    order_date,
    marketplace_id,
    SUM(revenue) AS revenue,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM fact_orders
GROUP BY 1, 2;

-- Weekly rollup from daily (smaller input = cheaper)
CREATE TABLE agg_weekly
WITH (format = 'PARQUET', location = 's3://bucket/agg/weekly/')
AS
SELECT
    DATE_TRUNC('week', CAST(order_date AS DATE)) AS week_start,
    marketplace_id,
    SUM(revenue) AS revenue
FROM agg_daily
GROUP BY 1, 2;
```

---

## 6. Operations & Lifecycle

### Table Maintenance (Vacuum / Compaction / Stats)

```sql
-- Iceberg compaction: merge small files
OPTIMIZE iceberg_orders REWRITE DATA USING BIN_PACK;

-- Targeted compaction for a specific partition
OPTIMIZE iceberg_orders REWRITE DATA USING BIN_PACK
WHERE order_date >= '2024-01-01';

-- Remove expired snapshots (prevents metadata bloat)
VACUUM iceberg_orders;

-- Set automatic expiry via table properties
ALTER TABLE iceberg_orders SET TBLPROPERTIES (
    'vacuum_min_snapshots_to_keep' = '5',
    'vacuum_max_snapshot_age_ms' = '604800000'  -- 7 days
);
```

- [Iceberg maintenance on Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-data-optimization.html)

### Data Retention & Archival

```sql
-- Delete old Iceberg partitions
DELETE FROM iceberg_orders WHERE order_date < DATE '2022-01-01';

-- Expire snapshots to reclaim S3 storage
VACUUM iceberg_orders;

-- For Hive-style tables: manage S3 lifecycle rules externally
-- aws s3api put-bucket-lifecycle-configuration to expire old partition prefixes
```

- [S3 Lifecycle management](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)

### Database-Level Operations

```sql
-- Iceberg time travel as point-in-time query (not a true backup)
SELECT * FROM iceberg_orders FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 00:00:00';

-- List snapshots to find restore point
SELECT snapshot_id, committed_at, operation
FROM "my_db"."iceberg_orders$snapshots"
ORDER BY committed_at DESC;

-- Restore to a specific snapshot
CALL system.rollback_to_snapshot('my_db', 'iceberg_orders', <snapshot_id>);
```

- [S3 versioning for backup](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)

### Cluster & Infrastructure Operations

Athena is fully serverless — no cluster to manage. Manage capacity via workgroups and service quotas.

```sql
-- Create workgroups to isolate teams and control resource usage
-- (done via Athena console or API, not SQL)

-- Set byte scan limit per query in a workgroup (auto-cancels expensive queries)
-- Workgroup setting: "Override client-side settings" + "Data scanned per query: 10 GB"

-- Check service quotas: concurrent query limit per account
-- aws service-quotas get-service-quota --service-code athena --quota-code L-xxx
```

- [Athena service quotas](https://docs.aws.amazon.com/athena/latest/ug/service-limits.html)
- [Athena workgroups](https://docs.aws.amazon.com/athena/latest/ug/manage-queries-control-costs-with-workgroups.html)

---

## 7. Observability

### Resource & Query Metrics

CloudWatch Metrics for Athena (available automatically, no setup required):

| Metric | Use |
|---|---|
| `DataScannedInBytes` | Cost proxy; alert on spikes |
| `QueryExecutionTime` | Latency tracking |
| `QueryQueueTime` | Concurrency issues |
| `QueryPlanningTime` | Metadata/planning overhead |
| `EngineExecutionTime` | Actual compute time |

```sql
-- Query execution details in Athena console
-- History tab: QueryExecutionId, Status, DataScannedInBytes, EngineExecutionTimeInMillis

-- Via AWS CLI
-- aws athena get-query-execution --query-execution-id <id>

-- List recent executions via API (paginated)
-- aws athena list-query-executions --work-group primary
```

- [Athena CloudWatch metrics](https://docs.aws.amazon.com/athena/latest/ug/query-metrics-viewing.html)

### Adoption & Throughput Metrics

```sql
-- Athena workgroups provide per-workgroup BytesScanned and query count metrics
-- Enable CloudWatch publishing in workgroup settings

-- Custom tracking: write query execution metadata to a tracking table
-- Use Athena API to pull query history and write to S3 → query via Athena
```

- [Athena workgroup metrics](https://docs.aws.amazon.com/athena/latest/ug/manage-queries-control-costs-with-workgroups.html)

### Execution Plan Inspection

```sql
-- EXPLAIN: static plan (no query execution)
EXPLAIN (FORMAT TEXT)
SELECT COUNT(*) FROM fact_orders WHERE order_date = '2024-01-15';

-- EXPLAIN ANALYZE: executes query and shows actual stats
EXPLAIN ANALYZE
SELECT COUNT(*) FROM fact_orders WHERE order_date = '2024-01-15';

-- Key things to look for:
-- "Estimates: rows=X, size=Y" vs actual: large mismatch = stale stats
-- "Distribution: BROADCAST" (good for small tables) vs "PARTITIONED" (shuffle)
-- "ScanFilterProject" with pushdown filters listed → confirms partition pruning
-- "LocalExchange" or "RemoteExchange" → shuffle points
```

- [Athena EXPLAIN](https://docs.aws.amazon.com/athena/latest/ug/athena-explain-statement.html)

### Alerts & Automation

```sql
-- Workgroup byte scan limit: auto-cancel queries exceeding threshold
-- Setting in console: Workgroup → Settings → Data usage controls
-- "Cancel query if it exceeds N GB"

-- CloudWatch alarm examples:
-- Alarm: DataScannedInBytes > 100 GB in 1 hour → notify via SNS
-- Alarm: QueryExecutionTime > 300 seconds → notify team

-- Auto-retry failed queries via Lambda trigger on CloudWatch Events
-- EventBridge rule: Athena query state change → FAILED → trigger Lambda retry
```

- [Athena workgroup data usage controls](https://docs.aws.amazon.com/athena/latest/ug/workgroups-setting-control-limits-cloudwatch.html)
- [Athena EventBridge integration](https://docs.aws.amazon.com/athena/latest/ug/monitoring-athena-with-cloudwatch.html)
