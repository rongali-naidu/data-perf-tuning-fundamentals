# AWS Glue Implementation Guide

> **Engine profile:** Serverless Spark ETL service. Glue ETL jobs run Spark on managed infrastructure billed per DPU-hour. Glue Data Catalog serves as the central metadata store for all S3-based tables, shared across Athena, Redshift Spectrum, and EMR. Cost driver: DPU-hours consumed per job run.

**Key references:**
- [AWS Glue developer guide](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)
- [Glue ETL best practices](https://docs.aws.amazon.com/glue/latest/dg/best-practices.html)
- [Glue DynamicFrame documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html)

---

## 1. Data Modeling & Schema Design

### Schema Pattern

Schema is defined in the Glue Data Catalog and shared across engines. Iceberg and Delta Table formats are supported for ACID operations.

```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read from Glue Catalog (star schema: fact + dimension)
fact_orders = glueContext.create_dynamic_frame.from_catalog(
    database="my_db",
    table_name="fact_orders"
).toDF()

dim_customer = glueContext.create_dynamic_frame.from_catalog(
    database="my_db",
    table_name="dim_customer"
).toDF()

# Join: broadcast small dimension to avoid shuffle
from pyspark.sql.functions import broadcast
result = fact_orders.join(broadcast(dim_customer), "customer_id")
```

### Aggregate & Rollup Design

Write pre-aggregated DataFrames to S3 and register in Glue Catalog. Schedule refresh via Glue Workflows or EventBridge.

```python
from pyspark.sql.functions import sum as spark_sum, count

daily_agg = (
    fact_orders
    .groupBy("order_date", "marketplace_id")
    .agg(
        spark_sum("revenue").alias("total_revenue"),
        count("*").alias("order_count")
    )
)

# Write to S3 and update Glue Catalog
glueContext.write_dynamic_frame.from_options(
    frame=glueContext.create_dynamic_frame_from_rdd(
        daily_agg.rdd, "daily_agg", glueContext
    ),
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://bucket/agg/daily/",
        "partitionKeys": ["order_date"]
    },
    transformation_ctx="write_daily_agg"
)
```

- [Glue Workflows](https://docs.aws.amazon.com/glue/latest/dg/workflows_overview.html)

### Time-Series & Trend Metrics

Use job bookmarks to prevent reprocessing old data. Window functions work identically to standard PySpark.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

w = Window.partitionBy("marketplace_id").orderBy("order_date")

df = spark.table("agg_daily_revenue").withColumn(
    "wow_growth",
    col("total_revenue") / lag("total_revenue", 7).over(w) - 1
)
```

- [Glue job bookmarks](https://docs.aws.amazon.com/glue/latest/dg/monitor-continuations.html)

### Slowly Changing Dimensions (SCD)

Glue has native SCD support in the ETL job studio, and Iceberg/Delta MERGE works identically to standard Spark.

```python
# Glue native SCD (available in Glue Studio visual ETL)
# Or use Iceberg MERGE for Type 2:
spark.sql("""
    MERGE INTO glue_catalog.my_db.dim_customer AS target
    USING staging_customer AS src
    ON target.customer_id = src.customer_id
       AND target.valid_to = '9999-12-31'
    WHEN MATCHED AND src.region != target.region
        THEN UPDATE SET valid_to = current_date() - interval 1 day
    WHEN NOT MATCHED
        THEN INSERT (customer_id, region, valid_from, valid_to)
             VALUES (src.customer_id, src.region, current_date(), '9999-12-31')
""")
```

- [Glue native SCD](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-scd.html)

### High-Cardinality Dimensions (Bridge Tables)

Same pattern as PySpark — pre-explode arrays in ETL jobs rather than at query time.

```python
from pyspark.sql.functions import explode

# Pre-explode at write time in Glue ETL job
bridge = (
    spark.table("raw_products")
    .select("product_id", explode("tags").alias("tag"))
)

# Write to Glue Catalog table
bridge.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "s3://bucket/bridge/product_tag/") \
    .saveAsTable("my_db.bridge_product_tag")
```

### Access Pattern Alignment

```python
# Glue Catalog stores column-level metadata usable for access pattern analysis
# Use Glue DataBrew or Athena queries against the catalog for profiling

# Check table partition structure
spark.sql("DESCRIBE EXTENDED my_db.fact_orders").show(50, truncate=False)

# Analyze query patterns via CloudWatch Athena metrics or Glue job logs
```

---

## 2. Physical Storage & Layout

### Storage Format

Configure output format in the Glue connection options. Glue supports Parquet, ORC, JSON, Iceberg, and Delta.

```python
# Write Parquet via GlueContext
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="parquet",
    format_options={"compression": "snappy"},
    connection_options={"path": "s3://bucket/output/", "partitionKeys": ["date"]},
)

# Write Iceberg via Spark (use spark session directly)
df.writeTo("glue_catalog.my_db.iceberg_orders") \
  .tableProperty("format-version", "2") \
  .partitionedBy("order_date") \
  .createOrReplace()

# Write Delta
df.write.format("delta").mode("overwrite").save("s3://bucket/delta/orders/")
```

- [Glue output formats](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format.html)
- [Glue Iceberg integration](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)

### File Size

Control file size via `repartition`/`coalesce` or Glue's `groupFiles` option for input.

```python
# Control output file count
df.coalesce(10).write.format("parquet").save("s3://...")  # reduce without full shuffle
df.repartition(100).write.format("parquet").save("s3://...")  # increase with shuffle

# Glue DynamicFrame: group small input files for better read performance
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://bucket/input/"],
        "groupFiles": "inPartition",    # group small files within same partition
        "groupSize": "134217728"        # target group size: 128 MB
    },
    format="parquet"
)
```

- [Glue file grouping](https://docs.aws.amazon.com/glue/latest/dg/grouping-input-files.html)

### Encoding & Compression

```python
# Parquet with Snappy (default)
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="parquet",
    format_options={"compression": "snappy"},
    connection_options={"path": "s3://bucket/output/"}
)

# Parquet with Zstd (better compression ratio)
format_options={"compression": "zstd"}

# ORC with Zlib
format="orc",
format_options={"compression": "zlib"}

# Verify: check S3 file sizes before/after compression change
# aws s3 ls s3://bucket/output/ | awk '{sum += $3} END {print sum/1024/1024, "MB"}'
```

### Partitioning & Distribution Strategy

```python
# Write partitioned output — Glue Catalog auto-updated with enableUpdateCatalog
glueContext.write_dynamic_frame.from_catalog(
    frame=dynamic_frame,
    database="my_db",
    table_name="fact_orders",
    additional_options={
        "enableUpdateCatalog": True,
        "partitionKeys": ["order_date", "marketplace_id"]
    }
)

# Using Spark write with Glue Catalog table
df.write \
    .format("parquet") \
    .partitionBy("order_date", "marketplace_id") \
    .mode("overwrite") \
    .option("path", "s3://bucket/fact_orders/") \
    .saveAsTable("my_db.fact_orders")
```

- [Glue partitioning](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-partitions.html)

### Data Types

Schema is defined in the Glue Catalog. Use Glue Schema Registry to enforce types on streaming data sources.

```python
# Define explicit schema to avoid type inference issues
from pyspark.sql.types import StructType, StructField, LongType, DateType, DecimalType

schema = StructType([
    StructField("order_id", LongType(), False),
    StructField("order_date", DateType(), False),
    StructField("revenue", DecimalType(18, 2), True)
])

df = spark.read.schema(schema).parquet("s3://bucket/raw/")

# Glue Schema Registry for Kafka/Kinesis streams
# https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html
```

- [Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)

### Clustering Structures & Indexes

```python
# Iceberg Z-ordering via Glue ETL job
spark.sql("""
    CALL glue_catalog.system.rewrite_data_files(
        table => 'my_db.iceberg_orders',
        strategy => 'sort',
        sort_order => 'order_date ASC, marketplace_id ASC'
    )
""")

# Delta Z-ordering
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")
dt.optimize().executeZOrderBy("order_date", "marketplace_id")

# Schedule via Glue Trigger to run after main ETL job completes
```

### Table Statistics

```python
# Update statistics after ETL job completes
spark.sql("ANALYZE TABLE my_db.fact_orders COMPUTE STATISTICS FOR ALL COLUMNS")

# For Glue Catalog tables: statistics available in catalog
# Check via: spark.sql("DESCRIBE EXTENDED my_db.fact_orders").show(100, False)

# Trigger stats update as final step in ETL job
def update_table_stats(database, table):
    spark.sql(f"ANALYZE TABLE {database}.{table} COMPUTE STATISTICS FOR ALL COLUMNS")
    print(f"Statistics updated for {database}.{table}")
```

- [Glue Data Catalog statistics](https://docs.aws.amazon.com/glue/latest/dg/data-catalog-table-statistics.html)

### Metadata (Open Table Formats)

```python
# Delta: inspect commit history
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")
dt.history().show(10)

# Iceberg: inspect snapshots
spark.sql("SELECT * FROM glue_catalog.my_db.iceberg_orders.snapshots").show()

# Expire old Delta files
dt.vacuum(retentionHours=168)  # 7 days

# Expire old Iceberg snapshots
spark.sql("""
    CALL glue_catalog.system.expire_snapshots(
        table => 'my_db.iceberg_orders',
        older_than => TIMESTAMP '2024-01-01 00:00:00',
        retain_last => 5
    )
""")
```

- [Glue Iceberg integration](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)

---

## 3. Data Ingestion

### Load Pattern (Bulk / Incremental / CDC)

```python
# Bulk load: full overwrite
df.write.format("parquet").mode("overwrite").partitionBy("order_date").save("s3://...")

# Incremental: use job bookmarks to process only new files
# In Glue job parameters: --job-bookmark-option job-bookmark-enable
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://bucket/raw/orders/"]},
    format="parquet",
    transformation_ctx="read_orders"  # bookmark tracks this context
)

# CDC: Iceberg MERGE for upsert/delete
spark.sql("""
    MERGE INTO glue_catalog.my_db.fact_orders AS target
    USING staging_orders AS src
    ON target.order_id = src.order_id
    WHEN MATCHED AND src.operation = 'DELETE' THEN DELETE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Streaming CDC from Kinesis
kinesis_df = spark.readStream \
    .format("kinesis") \
    .option("streamName", "orders-cdc") \
    .option("region", "us-east-1") \
    .option("startingPosition", "TRIM_HORIZON") \
    .load()
```

- [Glue job bookmarks](https://docs.aws.amazon.com/glue/latest/dg/monitor-continuations.html)
- [AWS DMS for CDC](https://docs.aws.amazon.com/dms/latest/userguide/Welcome.html)

### Source-Specific Considerations

```python
# Schema evolution: Glue Crawlers detect and update schema automatically
# Run Crawler after source schema changes

# Merge schema for evolving Parquet sources
df = spark.read.option("mergeSchema", "true").parquet("s3://bucket/raw/")

# Glue Schema Registry for enforced schema on Kafka/Kinesis
# Reject records that don't match registered schema version

# Handle bad records
from awsglue.dynamicframe import DynamicFrame

# ResolveChoice: handle ambiguous types
resolved = dynamic_frame.resolveChoice(
    specs=[("ambiguous_col", "cast:long")]
)

# Drop nulls or fill defaults
from awsglue.transforms import DropNullFields
cleaned = DropNullFields.apply(frame=dynamic_frame)
```

- [Glue Crawlers](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html)
- [Glue DynamicFrame resolveChoice](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html)

---

## 4. Query & Compute Execution

### Data Reduction

```python
# Pushdown predicates: filter applied at S3 read time
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="my_db",
    table_name="fact_orders",
    push_down_predicate="order_date >= '2024-01-01' AND marketplace_id = 1"
)
# This avoids loading filtered partitions into memory

# Select only required columns
dynamic_frame = dynamic_frame.select_fields(
    ["order_id", "customer_id", "revenue", "order_date"]
)

# Equivalent in Spark
df = spark.read.parquet("s3://bucket/fact_orders/") \
    .filter("order_date >= '2024-01-01' AND marketplace_id = 1") \
    .select("order_id", "customer_id", "revenue")
```

- [Glue pushdown predicates](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-partitions.html#aws-glue-programming-etl-partitions-pushdowns)

### Join Strategy

Same as PySpark — Glue runs Spark under the hood.

```python
from pyspark.sql.functions import broadcast

# Broadcast small dimension
result = fact_df.join(broadcast(dim_df), "customer_id")

# Configure broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)

# AQE (enabled by default in Glue 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### Data Movement & Shuffle

```python
# AQE settings for Glue — set as --conf job parameter or in script
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Cache repeated DataFrames to avoid re-reading from S3
from pyspark import StorageLevel
dim_df.persist(StorageLevel.MEMORY_AND_DISK)
# Use dim_df in multiple joins
dim_df.unpersist()
```

### Data Skew

```python
# Enable AQE skew join handling
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Manual salting for known skewed joins (same as PySpark chapter)
from pyspark.sql.functions import floor, rand, concat, lit, col, array, explode

SALT_FACTOR = 10

skewed_df = skewed_df.withColumn(
    "salted_key",
    concat(col("join_key"), lit("_"), (floor(rand() * SALT_FACTOR)).cast("string"))
)

small_df_exploded = small_df.withColumn(
    "salt_array", array([lit(str(i)) for i in range(SALT_FACTOR)])
).withColumn("salt", explode("salt_array")) \
 .withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt")))

result = skewed_df.join(small_df_exploded, "salted_key")
```

### Resource Allocation (Compute / Memory)

Glue worker types:

| Worker type | vCPU | Memory | Storage | Use case |
|---|---|---|---|---|
| G.1X | 4 | 16 GB | 64 GB | Standard ETL |
| G.2X | 8 | 32 GB | 128 GB | Memory-intensive joins/aggregations |
| G.4X | 16 | 64 GB | 256 GB | Large aggregations, broad shuffles |
| G.8X | 32 | 128 GB | 512 GB | Very large datasets, complex ML prep |
| G.025X | 2 | 4 GB | 64 GB | Light jobs, cost-sensitive workloads |

```python
# Configure in Glue job settings (console or API):
# --number-of-workers: number of worker nodes
# --worker-type: G.1X, G.2X, G.4X, G.8X, G.025X

# Override Spark memory in job script
spark.conf.set("spark.executor.memory", "28g")        # G.2X: 32 GB total
spark.conf.set("spark.executor.memoryOverhead", "4g")  # overhead for off-heap

# Check for spill in Spark UI (Glue job monitoring → View Spark UI)
# Stages → Shuffle Spill (Memory) / Shuffle Spill (Disk) columns
```

- [Glue worker types and DPUs](https://docs.aws.amazon.com/glue/latest/dg/add-job.html)

### Approximate Computation

```python
from pyspark.sql.functions import approx_count_distinct

# Approximate COUNT DISTINCT (~2% error, much faster)
df.agg(approx_count_distinct("customer_id", rsd=0.02).alias("approx_unique")).show()

# For pre-aggregated HLL: use max() + sum() pattern
# Daily sketch → weekly count: sum of daily sketches overestimates due to overlap
# Correct approach: merge HLL sketches if using native HLL library
```

### SQL Construct Choices

```python
# Glue uses Spark SQL — same behavior as PySpark chapter
# CTEs are inlined; use temp views for forced materialization

spark.sql("""
    SELECT customer_id, SUM(revenue) AS total
    FROM fact_orders GROUP BY 1
""").createOrReplaceTempView("customer_totals")
spark.catalog.cacheTable("customer_totals")

# Use UNION ALL, not UNION, when duplicates are impossible
df1.union(df2)       # Spark DataFrame union → equivalent to UNION ALL in SQL
# Note: DataFrame.union() does NOT deduplicate — use df1.union(df2).distinct() for UNION
```

### Execution Plan Analysis

```python
# Same as PySpark
df.explain(mode="formatted")

# Spark UI: accessible from Glue job monitoring
# Job run → View Spark UI → SQL tab for DAG with per-operator metrics
# CloudWatch logs contain explain output when printed to stderr
```

- [Glue job monitoring and debugging](https://docs.aws.amazon.com/glue/latest/dg/monitor-debug-ui.html)

---

## 5. Precomputation & Reuse

### Materialized Views

```python
# Glue ETL job writes pre-computed result to S3
# Schedule via Glue Trigger or EventBridge

def refresh_materialized_view(spark, source_table, target_path, target_table):
    df = spark.sql(f"""
        SELECT order_date, marketplace_id,
               SUM(revenue) AS total_revenue, COUNT(*) AS order_count
        FROM {source_table}
        GROUP BY 1, 2
    """)
    df.write.format("parquet") \
        .mode("overwrite") \
        .partitionBy("order_date") \
        .save(target_path)
    # Register in Glue Catalog
    spark.catalog.refreshTable(target_table)
```

- [Glue Workflows for dependency management](https://docs.aws.amazon.com/glue/latest/dg/workflows_overview.html)

### Engine-Level Caching

```python
# Cache DataFrame in memory (Spark-level cache, not persistent)
df.cache()

# Job bookmarks: skip already-processed files (persistent across runs)
# Enable in Glue job: --job-bookmark-option job-bookmark-enable
# Bookmark state stored in Glue — survives job restarts
# Reset bookmark: aws glue reset-job-bookmark --job-name my-job

# Glue result caching: not available — each job run recomputes
# For repeated reads, cache to S3 as an intermediate table
```

- [Glue job bookmarks](https://docs.aws.amazon.com/glue/latest/dg/monitor-continuations.html)

### Pre-aggregated Tables

```python
# Glue Workflow: chain ETL jobs for aggregation tiers
# raw_loader → daily_agg_job → weekly_agg_job → monthly_agg_job

# Each job reads from the previous tier
def build_weekly_agg(spark):
    daily = spark.read.format("parquet").load("s3://bucket/agg/daily/")
    weekly = daily \
        .withColumn("week_start", date_trunc("week", col("order_date"))) \
        .groupBy("week_start", "marketplace_id") \
        .agg(spark_sum("total_revenue").alias("revenue"))
    weekly.write.format("parquet").mode("overwrite") \
          .partitionBy("week_start").save("s3://bucket/agg/weekly/")
```

---

## 6. Operations & Lifecycle

### Table Maintenance (Vacuum / Compaction / Stats)

```python
# Iceberg compaction via Glue ETL job (schedule as periodic maintenance)
spark.sql("""
    CALL glue_catalog.system.rewrite_data_files(
        table => 'my_db.iceberg_orders',
        strategy => 'binpack',
        options => map('target-file-size-bytes', '134217728')  -- 128 MB
    )
""")

# Delta compaction
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")
dt.optimize().executeCompaction()

# Update statistics
spark.sql("ANALYZE TABLE my_db.fact_orders COMPUTE STATISTICS FOR ALL COLUMNS")

# Schedule all three as sequential steps in a maintenance Glue job
# Trigger: time-based (e.g., daily at 2 AM) or after main ETL job completion
```

### Data Retention & Archival

```python
# Iceberg snapshot expiry
spark.sql("""
    CALL glue_catalog.system.expire_snapshots(
        table => 'my_db.iceberg_orders',
        older_than => TIMESTAMP '2024-01-01 00:00:00',
        retain_last => 5
    )
""")

# Delta file cleanup
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")
dt.vacuum(retentionHours=168)  # 7 days minimum

# Delete old partitions
spark.sql("DELETE FROM glue_catalog.my_db.iceberg_orders WHERE order_date < '2022-01-01'")
dt.delete("order_date < '2022-01-01'")  # Delta

# S3 Lifecycle policy for underlying data files
# Set up via S3 console or CLI — moves files to Glacier after X days
```

- [S3 Lifecycle rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)

### Database-Level Operations

```python
# Export Glue Catalog metadata as backup
# aws glue get-tables --database-name my_db > catalog_backup.json

# Delta time travel (audit trail, not backup)
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")
dt.history().show(10)

# Read historical snapshot
df = spark.read.format("delta") \
    .option("versionAsOf", 10) \
    .load("s3://bucket/delta/orders/")

# Iceberg time travel
df = spark.read.format("iceberg") \
    .option("as-of-timestamp", "2024-01-15T00:00:00+00:00") \
    .load("s3://bucket/iceberg/orders/")
```

- [Glue Catalog backup](https://docs.aws.amazon.com/glue/latest/dg/backup-restore-glue-catalog.html)

### Cluster & Infrastructure Operations

```python
# Glue is serverless — no cluster management
# Key tuning levers:

# 1. Worker type: match to job memory requirements
#    - G.1X for standard ETL
#    - G.2X+ for large joins/aggregations

# 2. Number of workers: match to data parallelism
#    Rule of thumb: 1 worker per 2-4 GB of compressed input data

# 3. Dynamic allocation: auto-scale within min/max bounds
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")

# 4. Timeout: set appropriate job timeout to auto-kill runaway jobs
# Glue job setting: --timeout 60 (minutes)
```

- [Glue DPU management](https://docs.aws.amazon.com/glue/latest/dg/add-job.html)

---

## 7. Observability

### Resource & Query Metrics

CloudWatch Metrics for Glue (available automatically):

| Metric | Namespace | Use |
|---|---|---|
| `glue.driver.aggregate.bytesRead` | `Glue` | Input data volume |
| `glue.driver.aggregate.recordsRead` | `Glue` | Input record count |
| `glue.driver.aggregate.shuffleLocalBytesRead` | `Glue` | Shuffle volume proxy |
| `glue.ALL.jvm.heap.used` | `Glue` | Memory pressure |
| `glue.ALL.s3.filesystem.read_bytes` | `Glue` | S3 I/O volume |

```python
# Print job metrics from within script
from awsglue.context import GlueContext

job_name = args['JOB_NAME']
run_id = args['JOB_RUN_ID']

# Log custom metrics to CloudWatch via print (captured by Glue logging)
print(f"JOB_METRIC: rows_processed={df.count()}, job={job_name}, run={run_id}")

# Access Spark UI: Glue job monitoring → "Run" tab → "View Spark UI"
```

- [Glue CloudWatch metrics](https://docs.aws.amazon.com/glue/latest/dg/monitoring-awsglue-with-cloudwatch-metrics.html)

### Adoption & Throughput Metrics

```python
# Job run history: AWS console → Glue → ETL Jobs → select job → Run history
# Via CLI: aws glue get-job-runs --job-name my-job

# Write job-level metrics to a tracking table for trend analysis
import boto3
from datetime import datetime

def log_job_metrics(job_name, run_id, rows_in, rows_out, start_time):
    duration_sec = int((datetime.now() - start_time).total_seconds())
    metrics_df = spark.createDataFrame([{
        "job_name": job_name,
        "run_id": run_id,
        "run_date": str(datetime.now().date()),
        "rows_input": rows_in,
        "rows_output": rows_out,
        "duration_sec": duration_sec
    }])
    metrics_df.write.format("parquet").mode("append").save("s3://bucket/metrics/glue_jobs/")
```

- [Glue job monitoring](https://docs.aws.amazon.com/glue/latest/dg/monitor-glue.html)

### Execution Plan Inspection

```python
# Standard Spark explain — output appears in CloudWatch logs
df.explain(mode="formatted")

# Access Spark UI for visual DAG
# Glue Console → ETL Jobs → select job → Run → View Spark UI
# SQL tab: DAG with per-operator metrics (rows, bytes, shuffle, spill)

# Key indicators in Glue Spark UI:
# - PartitionFilters: [] → empty means no pushdown; check push_down_predicate
# - BroadcastHashJoin vs SortMergeJoin → verify broadcast threshold
# - Exchange nodes → count shuffles; each is a performance cost
```

### Alerts & Automation

```python
# Glue job failure notification via SNS
# Configure in Glue job settings: "Job notifications" → notify on FAILED/TIMEOUT

# EventBridge rule to trigger Lambda on Glue job state change
# EventBridge pattern:
# {
#   "source": ["aws.glue"],
#   "detail-type": ["Glue Job State Change"],
#   "detail": {"jobName": ["my-etl-job"], "state": ["FAILED", "TIMEOUT"]}
# }

# In-job alerting: catch exceptions and publish to SNS
import boto3
import sys

def handle_job_failure(job_name, error_message):
    sns = boto3.client("sns", region_name="us-east-1")
    sns.publish(
        TopicArn="arn:aws:sns:us-east-1:123456789:data-alerts",
        Subject=f"Glue job failed: {job_name}",
        Message=f"Error: {error_message}\nJob: {job_name}"
    )
    sys.exit(1)

try:
    main()
except Exception as e:
    handle_job_failure(args['JOB_NAME'], str(e))
    raise
```

- [Glue job notifications](https://docs.aws.amazon.com/glue/latest/dg/glue-notifications.html)
- [EventBridge for Glue](https://docs.aws.amazon.com/glue/latest/dg/glue-notifications.html)
