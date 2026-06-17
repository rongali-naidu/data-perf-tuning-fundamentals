# PySpark / Cradle Implementation Guide

> **Engine profile:** Distributed Spark on Cradle (Amazon's managed Spark platform). Jobs run on a cluster provisioned per-job. Driver coordinates; executors process partitions in parallel. Cost driver: cluster runtime (instance type × number of nodes × duration). Data lives on S3 as Parquet, Delta, or Iceberg.

**Key references:**
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Spark Tuning Guide](https://spark.apache.org/docs/latest/tuning.html)
- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)


---

## 1. Data Modeling & Schema Design

### Schema Pattern

Star schema works well in Spark. For very large dimension tables that can't be broadcast, consider OBT (One Big Table) to eliminate joins entirely — storage is cheap, shuffle is not.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.getOrCreate()

# Small dimension: broadcast to eliminate shuffle
dim_customer = spark.table("dim_customer")  # small table
fact_orders = spark.table("fact_orders")    # large table

result = fact_orders.join(broadcast(dim_customer), "customer_id")
```

### Aggregate & Rollup Design

Write pre-aggregated DataFrames to Parquet/Delta. Orchestrate refresh via Cradle pipeline dependencies so downstream jobs wait for upstream aggregation to complete.

```python
from pyspark.sql.functions import col, sum as spark_sum, count

daily_agg = (
    spark.table("fact_orders")
    .groupBy("order_date", "marketplace_id")
    .agg(
        spark_sum("revenue").alias("total_revenue"),
        count("*").alias("order_count")
    )
)

# Write as Delta for easy incremental updates
daily_agg.write.format("delta").mode("overwrite").save("s3://bucket/agg/daily/")
```

### Time-Series & Trend Metrics

Use `Window` functions to compute WoW/MoM without self-joins. Partition data by date at write time to limit scan scope.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col

w = Window.orderBy("order_date")

df = spark.table("agg_daily_revenue").withColumn(
    "revenue_7d_ago", lag("total_revenue", 7).over(w)
).withColumn(
    "wow_growth", col("total_revenue") / col("revenue_7d_ago") - 1
)
```

- [PySpark Window functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)

### Slowly Changing Dimensions (SCD)

Delta Lake `MERGE INTO` handles Type 2 upserts — open/close validity windows in one statement.

```python
from delta.tables import DeltaTable

dim_table = DeltaTable.forPath(spark, "s3://bucket/dim/customer/")
staged = spark.table("staging_customer")

dim_table.alias("target").merge(
    staged.alias("src"),
    "target.customer_id = src.customer_id AND target.valid_to = '9999-12-31'"
).whenMatchedUpdate(
    condition="src.region != target.region",
    set={"valid_to": "current_date() - interval 1 day"}
).whenNotMatchedInsert(
    values={
        "customer_id": "src.customer_id",
        "region": "src.region",
        "valid_from": "current_date()",
        "valid_to": "'9999-12-31'"
    }
).execute()
```

- [Delta Lake MERGE](https://docs.delta.io/latest/delta-update.html)

### High-Cardinality Dimensions (Bridge Tables)

Pre-explode arrays into bridge tables at write time. Avoid `explode()` at query time on large fact DataFrames — row count multiplication at scale is expensive.

```python
from pyspark.sql.functions import explode, col

# At write time (ETL job): pre-explode product tags into bridge table
bridge = (
    spark.table("raw_products")
    .select("product_id", explode("tags").alias("tag"))
)
bridge.write.format("parquet").partitionBy("tag").save("s3://bucket/bridge/product_tag/")

# At query time: join on bridge table (no explode needed)
result = fact_sales.join(bridge, "product_id").join(dim_tag, "tag")
```

### Access Pattern Alignment

Use Spark History Server to analyze which columns appear most in filter predicates and join keys. Bucket/partition write-time data to match.

---

## 2. Physical Storage & Layout

### Storage Format

Default to Parquet. Use Delta for tables that need ACID guarantees (upserts, deletes, schema evolution). Use Iceberg when cross-engine compatibility (Athena, Redshift Spectrum) matters.

```python
# Write Parquet (columnar, compressed)
df.write.format("parquet").mode("overwrite").save("s3://bucket/table/")

# Write Delta (ACID, time travel, schema evolution)
df.write.format("delta").mode("overwrite").save("s3://bucket/delta/table/")

# Write partitioned Delta
df.write.format("delta").partitionBy("year", "month").mode("overwrite").save("s3://...")

# Read
df = spark.read.format("delta").load("s3://bucket/delta/table/")
```

- [Spark Parquet data source](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- [Delta Lake quickstart](https://docs.delta.io/latest/quick-start.html)

### File Size

Target ~128 MB per output file. `repartition()` increases file count with a full shuffle. `coalesce()` reduces file count without full shuffle — use when reducing, especially after a filter.

```python
# Control output file count
df.repartition(100).write.parquet("s3://...")     # 100 files — full shuffle
df.coalesce(10).write.parquet("s3://...")         # 10 files — no shuffle

# Repartition by column value (one file per partition key group)
df.repartitionByRange(100, "order_date").write.partitionBy("order_date").parquet("s3://...")

# Spark config: auto-size post-shuffle partitions (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

- [DataFrame.repartition](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html)

### Encoding & Compression

Parquet handles column-level encoding internally (dictionary, RLE, delta). Choose file-level compression codec:

```python
# Snappy: fast compression/decompression, moderate ratio — default
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# Zstd: better compression ratio, comparable speed — good for cold storage
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

# Gzip: highest ratio, slowest — avoid for frequently read tables
spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
```

- [Spark SQL performance tuning (compression)](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

### Partitioning & Distribution Strategy

Partition write-time data on high-selectivity filter columns. Bucket on join keys for tables that are always joined together (eliminates shuffle on join).

```python
# Partition by date + region for typical filter patterns
df.write \
    .partitionBy("order_date", "marketplace_id") \
    .format("parquet") \
    .save("s3://bucket/fact_orders/")

# Bucketing: co-locate matching join keys (eliminates shuffle at join time)
# Both tables must use same number of buckets and same key
df.write \
    .bucketBy(200, "customer_id") \
    .sortBy("customer_id") \
    .format("parquet") \
    .saveAsTable("fact_orders_bucketed")
```

- [Spark performance tuning — partitioning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

### Data Types

Define schema explicitly. Avoid `inferSchema=True` in production — it reads the entire dataset to infer types and defaults to overly wide types (e.g., `LongType` for integers).

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType

schema = StructType([
    StructField("order_id", IntegerType(), False),       # INT not BIGINT
    StructField("order_date", DateType(), False),         # DATE not STRING
    StructField("revenue", DecimalType(18, 2), True),
    StructField("marketplace_id", IntegerType(), False),
])

df = spark.read.schema(schema).parquet("s3://...")
```

- [Spark SQL data types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)

### Clustering Structures & Indexes

Spark/Delta: Z-ordering clusters data for multi-dimensional range queries, creating effective min/max zone maps per file.

```python
from delta.tables import DeltaTable

# Z-order on columns frequently used together in range filters
DeltaTable.forPath(spark, "s3://bucket/fact_orders/").optimize().executeZOrderBy("order_date", "marketplace_id")
# After Z-ordering: queries filtering on both columns skip many more files

# Bloom filters for point lookups on high-cardinality columns
spark.conf.set("spark.databricks.io.cache.enabled", "true")  # if available
# Or set at table level:
# ALTER TABLE fact_orders SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '5')
```

- [Delta Lake optimizations (Z-ordering)](https://docs.delta.io/latest/optimizations-oss.html)

### Table Statistics

Collect statistics so the query optimizer can make accurate join order and strategy decisions.

```python
# Collect statistics for all columns
spark.sql("ANALYZE TABLE fact_orders COMPUTE STATISTICS FOR ALL COLUMNS")

# Collect for specific columns only (faster)
spark.sql("ANALYZE TABLE fact_orders COMPUTE STATISTICS FOR COLUMNS order_date, customer_id, marketplace_id")

# Check estimated row count in plan
df.explain(mode="cost")  # shows row count estimates with cost info
```

- [Spark SQL statistics collection](https://spark.apache.org/docs/latest/sql-performance-tuning.html#collecting-table-statistics)

### Metadata (Open Table Formats)

```python
from delta.tables import DeltaTable

# Delta: inspect commit history
dt = DeltaTable.forPath(spark, "s3://bucket/delta/table/")
dt.history().show(10)

# Iceberg: inspect snapshots
spark.sql("SELECT * FROM catalog.db.table.snapshots").show()

# Delta: vacuum old files (default retention: 7 days)
dt.vacuum(retentionHours=168)  # 7 days

# Delta: configure log retention
spark.sql("ALTER TABLE delta.`s3://bucket/delta/table/` SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days')")
```

- [Delta Lake utility operations](https://docs.delta.io/latest/delta-utility.html)

---

## 3. Data Ingestion

### Load Pattern (Bulk / Incremental / CDC)

```python
# Bulk load: overwrite entire table
df.write.format("delta").mode("overwrite").save("s3://bucket/delta/orders/")

# Incremental append: add only new records
new_records = spark.read.parquet("s3://bucket/raw/orders/2024/01/15/")
new_records.write.format("delta").mode("append").save("s3://bucket/delta/orders/")

# Incremental upsert (CDC): MERGE on primary key
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")
target.alias("t").merge(
    new_records.alias("s"),
    "t.order_id = s.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Streaming CDC from Kinesis
df_stream = spark.readStream \
    .format("kinesis") \
    .option("streamName", "orders-cdc") \
    .option("region", "us-east-1") \
    .load()
```

- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

### Source-Specific Considerations

```python
# Handle schema drift: merge schemas when reading Parquet
df = spark.read.option("mergeSchema", "true").parquet("s3://bucket/raw/orders/")

# Late-arriving data: watermark in Structured Streaming
from pyspark.sql.functions import col

df_watermarked = df_stream \
    .withWatermark("event_time", "2 hours") \
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("marketplace_id")
    ).count()

# Cradle-specific: partition filter evidence in driver log
# Look for: "Found N partition filters to apply: EqualsAndesFilter(marketplace_id,1)..."
# This confirms predicate pushdown is active against Andes (internal data store)
```

- [Spark schema merging](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#schema-merging)

---

## 4. Query & Compute Execution

### Data Reduction

Partition pruning is verified via the driver log in Cradle. Functions on partition columns disable pruning.

```python
# Good: filter directly on partition column — pruning active
df = spark.read.parquet("s3://bucket/fact_orders/") \
    .filter("order_date >= '2024-01-01' AND marketplace_id = 1")

# Bad: function on partition column — pruning disabled
df = spark.read.parquet("s3://bucket/fact_orders/") \
    .filter("year(order_date) = 2024")  # Spark cannot push this to partition filter

# Verify pushdown in driver log (Cradle):
# "Found 2 partition filters to apply: EqualsAndesFilter(marketplace_id,1),
#  GreaterThanEqualsAndesFilter(order_date,2024-01-01T00:00:00.000Z)"

# Select only required columns (projection pruning)
df = spark.read.parquet("s3://...").select("order_id", "customer_id", "revenue")
```

### Join Strategy

```python
from pyspark.sql.functions import broadcast

# Broadcast small table: zero shuffle
result = large_df.join(broadcast(small_df), "key")

# Configure broadcast threshold (default: 10 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50 MB

# SQL join hints
spark.sql("""
    SELECT /*+ BROADCAST(d) */ f.*, d.region
    FROM fact_orders f
    JOIN dim_customer d ON f.customer_id = d.customer_id
""")

# Other hints:
# /*+ MERGE(t) */   → force Sort-Merge Join
# /*+ SHUFFLE_HASH(t) */ → force Shuffle Hash Join
# /*+ SHUFFLE_REPLICATE_NL(t) */ → force Nested Loop (avoid in production)
```

- [Spark SQL join hints](https://spark.apache.org/docs/3.0.0/sql-ref-syntax-qry-select-hints.html)
- [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)

### Data Movement & Shuffle

```python
# AQE: automatically coalesces shuffle partitions and picks join strategies
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Set shuffle partition count explicitly for non-AQE jobs
spark.conf.set("spark.sql.shuffle.partitions", "200")  # default 200, tune to data size

# Cache to avoid re-shuffle on repeated use of same DataFrame
from pyspark.storagelevel import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)
# ... use df multiple times ...
df.unpersist()  # release when done

# Check shuffle bytes in Spark UI: Stages tab → Shuffle Read/Write columns
```

- [AQE documentation](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)

### Data Skew

```python
# Option 1: AQE automatic skew join handling
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")

# Option 2: Salting — add random prefix to distribute hot keys
from pyspark.sql.functions import col, concat, lit, floor, rand, lpad

SALT_FACTOR = 10

# Salt the large (skewed) DataFrame
skewed_df = skewed_df.withColumn(
    "salt", (floor(rand() * SALT_FACTOR)).cast("string")
).withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt")))

# Explode the small DataFrame to match all salt values
from pyspark.sql.functions import array, explode
small_df = small_df.withColumn(
    "salt_array", array([lit(str(i)) for i in range(SALT_FACTOR)])
).withColumn("salt", explode("salt_array")) \
 .withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt")))

result = skewed_df.join(small_df, "salted_key")

# Option 3: Handle null join keys separately
non_null = df.filter(col("join_key").isNotNull())
null_rows = df.filter(col("join_key").isNull())
result = non_null.join(other_df, "join_key").union(null_rows)
```

- [AQE skew join optimization](https://spark.apache.org/docs/latest/sql-performance-tuning.html#skew-join-optimization)
- [Salting technique explained](https://towardsdatascience.com/salting-your-spark-to-avoid-skew-aa987185a1d2)

### Resource Allocation (Compute / Memory)

```python
# Key memory configurations in Cradle job definition:
spark.conf.set("spark.executor.memory", "8g")       # heap per executor
spark.conf.set("spark.executor.memoryOverhead", "2g") # off-heap (native memory, Python UDFs)
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.memory.fraction", "0.8")       # fraction of heap for execution+storage
spark.conf.set("spark.memory.storageFraction", "0.3") # portion of above reserved for storage

# Detect spill to disk in Spark UI:
# Stages tab → look for non-zero "Shuffle Spill (Memory)" and "Shuffle Spill (Disk)" columns

# Reduce shuffle partition count to increase data per partition (less spill risk)
spark.conf.set("spark.sql.shuffle.partitions", "100")
```

- [Spark memory management](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)

### Approximate Computation

```python
from pyspark.sql.functions import approx_count_distinct, col

# Approximate COUNT DISTINCT: ~1-2% error, much faster
df.select(approx_count_distinct("customer_id", rsd=0.02).alias("approx_unique_customers"))

# For pre-aggregated HLL sketches across multiple datasets:
# Use max() + sum() pattern instead of count(distinct)
# Example: daily_agg has pre-computed HLL sketches per segment
# Use max() per segment, then sum() across segments — avoids recounting
daily_agg.groupBy("week").agg(
    spark_sum("daily_unique_customers").alias("weekly_unique_customers")  # WRONG: double-counts
)
# Correct: if using HLL sketches, aggregate the sketch, then get count from merged sketch
```

- [approx_count_distinct](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.approx_count_distinct.html)
- [HyperLogLog at Facebook](https://engineering.fb.com/2018/12/13/data-infrastructure/hyperloglog/)

### SQL Construct Choices

```python
# CTEs in Spark SQL are inlined (not materialized) — may be re-executed multiple times
# Use temp views to force single execution
spark.sql("""
    WITH expensive AS (
        SELECT customer_id, SUM(revenue) AS total
        FROM fact_orders GROUP BY 1
    )
    SELECT * FROM expensive WHERE total > 1000
""")
# If 'expensive' is referenced twice, Spark may compute it twice

# Better: cache as temp view
spark.sql("""
    SELECT customer_id, SUM(revenue) AS total
    FROM fact_orders GROUP BY 1
""").createOrReplaceTempView("expensive_cached")
spark.catalog.cacheTable("expensive_cached")

# UNION ALL vs UNION
spark.sql("SELECT id FROM a UNION ALL SELECT id FROM b")  # no dedup — preferred
spark.sql("SELECT id FROM a UNION SELECT id FROM b")       # dedup — adds sort/hash step
```

- [Spark SQL WITH clause (CTEs)](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-cte.html)

### Execution Plan Analysis

```python
# Basic explain
df.explain()

# Extended: includes parsed, analyzed, optimized, and physical plans
df.explain(mode="extended")

# Cost-based: shows row count and size estimates
df.explain(mode="cost")

# Formatted: structured output easier to read
df.explain(mode="formatted")

# Spark UI: SQL tab shows physical plan as a DAG with per-operator metrics
# Access at: http://driver:4040/SQL/ during job execution
# Or Spark History Server after job completes

# Key things to look for in physical plan:
# - FileScan with PartitionFilters: [] → empty means no partition pruning
# - BroadcastHashJoin vs SortMergeJoin → SMJ with a small table means missed broadcast
# - Exchange (shuffle): count these — each is a performance cost
# - Sort vs SortMergeJoin → external sort means memory insufficient
```

- [DataFrame.explain()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html)
- [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html)

---

## 5. Precomputation & Reuse

### Materialized Views

Spark has no native materialized views. Use pre-computed Delta/Parquet tables refreshed on a schedule via Cradle pipeline dependencies.

```python
# Pre-compute and write (the "materialized view")
daily_agg = (
    spark.table("fact_orders")
    .filter("order_date >= date_sub(current_date(), 90)")
    .groupBy("order_date", "marketplace_id")
    .agg(spark_sum("revenue").alias("total_revenue"))
)
daily_agg.write.format("delta").mode("overwrite").save("s3://bucket/mv/daily_revenue/")

# Downstream jobs read from the pre-computed table
result = spark.read.format("delta").load("s3://bucket/mv/daily_revenue/")
```

### Engine-Level Caching

```python
from pyspark import StorageLevel

# Cache in memory (fast, but evicted under memory pressure)
df.cache()  # equivalent to persist(MEMORY_AND_DISK)

# Explicit storage level
df.persist(StorageLevel.MEMORY_AND_DISK)   # spill to disk if memory full
df.persist(StorageLevel.MEMORY_ONLY)        # faster, dropped if memory full
df.persist(StorageLevel.DISK_ONLY)          # always on disk

# Check cached tables in Spark UI: Storage tab
# Unpersist when done to free memory
df.unpersist()

# Cache a SQL temp view
spark.catalog.cacheTable("my_temp_view")
spark.catalog.uncacheTable("my_temp_view")
```

- [RDD persistence (StorageLevels)](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)

### Pre-aggregated Tables

```python
# Build dependency chain: raw → daily → weekly
# Each Cradle job depends on the previous tier completing

# Tier 1: daily aggregation job
def build_daily_agg(execution_date):
    df = spark.read.parquet(f"s3://bucket/raw/orders/{execution_date}/")
    agg = df.groupBy("marketplace_id").agg(spark_sum("revenue").alias("revenue"))
    agg.withColumn("date", lit(execution_date)) \
       .write.format("delta").mode("append").save("s3://bucket/agg/daily/")

# Tier 2: weekly rollup — depends on Tier 1 for all 7 days
def build_weekly_agg(week_start):
    df = spark.read.format("delta").load("s3://bucket/agg/daily/") \
              .filter(f"date >= '{week_start}' AND date < date_add('{week_start}', 7)")
    agg = df.groupBy("marketplace_id").agg(spark_sum("revenue").alias("revenue"))
    agg.withColumn("week_start", lit(week_start)) \
       .write.format("delta").mode("append").save("s3://bucket/agg/weekly/")
```

---

## 6. Operations & Lifecycle

### Table Maintenance (Vacuum / Compaction / Stats)

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")

# Compact small files
dt.optimize().executeCompaction()

# Z-order and compact in one step
dt.optimize().executeZOrderBy("order_date", "marketplace_id")

# Remove files older than retention period (default: 7 days)
dt.vacuum(retentionHours=168)  # 7 days minimum recommended

# Update statistics after major writes
spark.sql("ANALYZE TABLE delta.`s3://bucket/delta/orders/` COMPUTE STATISTICS FOR ALL COLUMNS")
```

- [Delta Lake utility operations](https://docs.delta.io/latest/delta-utility.html)

### Data Retention & Archival

```python
from delta.tables import DeltaTable

# Configure log and file retention
spark.sql("""
    ALTER TABLE delta.`s3://bucket/delta/orders/`
    SET TBLPROPERTIES (
        'delta.logRetentionDuration' = 'interval 30 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")

# Delete old partitions (with MERGE to handle in Delta)
dt = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")
dt.delete("order_date < '2022-01-01'")

# Then vacuum to reclaim physical storage
dt.vacuum(retentionHours=168)

# Iceberg retention (if using Iceberg)
spark.sql("""
    ALTER TABLE catalog.db.orders SET TBLPROPERTIES (
        'history.expire.min-snapshots-to-keep' = '5',
        'history.expire.max-snapshot-age-ms' = '604800000'  -- 7 days
    )
""")
```

- [Delta vacuum](https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table)

### Database-Level Operations

```python
# Time travel: access historical snapshot (audit, not backup)
# Delta: read as of version
df = spark.read.format("delta").option("versionAsOf", 10).load("s3://bucket/delta/orders/")

# Delta: read as of timestamp
df = spark.read.format("delta").option("timestampAsOf", "2024-01-15").load("s3://...")

# Describe history to find versions
spark.sql("DESCRIBE HISTORY delta.`s3://bucket/delta/orders/`").show(10)

# For true backups: copy data files to a separate S3 prefix before major operations
# aws s3 sync s3://bucket/delta/orders/ s3://backup-bucket/delta/orders-backup-20240115/
```

### Cluster & Infrastructure Operations

Cradle manages cluster lifecycle. Configure cluster size in the job definition. Monitor via Cradle console.

```python
# Key Spark configs for Cradle job sizing:
# In job config / spark-defaults.conf:
#   spark.executor.instances = 20
#   spark.executor.cores = 4
#   spark.executor.memory = 8g
#   spark.driver.memory = 4g

# Dynamic allocation (auto-scales executors based on workload):
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")
```

---

## 7. Observability

### Resource & Query Metrics

```python
# Spark UI (available at http://driver:4040 during job execution)
# Key tabs:
# - Jobs: overall job progress
# - Stages: per-stage task durations, shuffle read/write, spill
# - Storage: cached DataFrames and their memory usage
# - Executors: per-executor memory, GC time, task count
# - SQL: visual DAG for SQL queries with per-operator metrics

# Programmatic: add custom metrics via accumulator
counter = spark.sparkContext.accumulator(0, "rows_processed")
df.foreach(lambda row: counter.add(1))
print(f"Rows processed: {counter.value}")

# Log execution metrics programmatically
import time
start = time.time()
result = df.count()
print(f"Count took {time.time() - start:.1f}s")
```

- [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html)

### Adoption & Throughput Metrics

Use Spark History Server for completed job metrics. Cradle console provides pipeline-level SLA tracking and job run history.

```python
# Log job-level metrics to a tracking table
from datetime import datetime

metrics = spark.createDataFrame([{
    "job_name": "daily_agg_job",
    "run_date": str(datetime.now().date()),
    "input_rows": input_df.count(),
    "output_rows": output_df.count(),
    "duration_sec": int(time.time() - start_time)
}])
metrics.write.format("delta").mode("append").save("s3://bucket/metrics/job_runs/")
```

### Execution Plan Inspection

See [Execution Plan Analysis](#execution-plan-analysis) above.

**Quick checklist for plan review:**
1. `FileScan`: are `PartitionFilters` populated? Empty = no pruning.
2. Join type: `BroadcastHashJoin` (good) or `SortMergeJoin` on a small table (missed broadcast)?
3. `Exchange` (shuffle) count: minimize these.
4. `Sort` operator: is it spilling? Check `Spill (Memory)` and `Spill (Disk)` in Stages tab.

### Alerts & Automation

```python
# Cradle pipeline alerts: configure in job definition for failure/timeout notification
# CloudWatch alarms on Spark job metrics (EMR/Glue-based)

# In-job alerting: catch and log job failures with context
import sys
import traceback

try:
    result = spark.sql("SELECT ...")
    result.write.save("s3://...")
except Exception as e:
    print(f"JOB FAILED: {str(e)}", file=sys.stderr)
    traceback.print_exc()
    # Publish to SNS or write failure record to DynamoDB for alerting
    raise  # re-raise so Cradle marks job as failed

# Custom SparkListener for in-job monitoring
from pyspark import SparkContext
# Implement SparkListener subclass to capture stage completion events
```

- [Spark monitoring and instrumentation](https://spark.apache.org/docs/latest/monitoring.html)
