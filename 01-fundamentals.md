# Fundamentals Reference

Engine-agnostic principles. Every sub-criteria answers three questions:
- **What is the ideal state?** — what good looks like
- **Why does it matter?** — what goes wrong without it
- **How to inspect?** — how to verify you're meeting it on any engine

For engine-specific implementation, see the engine chapters.

---

## 1. Data Modeling & Schema Design

Design decisions made before data is written. Getting these right eliminates entire classes of performance problems downstream.

| Sub-criteria | Ideal State | Why It Matters | How to Inspect |
|---|---|---|---|
| **Schema Pattern** | Schema matches dominant access pattern. Join count is minimized. Denormalization is intentional, not accidental. | Wrong schema forces joins the engine wasn't designed for, or over-normalizes causing fan-out. | Count joins per production query in query logs. Measure p50/p99 latency across schema options. Review query plans for join depth. |
| **Aggregate & Rollup Design** | Repeated aggregation patterns are eliminated through pre-computation. Downstream queries hit small aggregated tables, not large fact tables. | Every user repeating the same expensive aggregation wastes compute and creates latency. | Identify repeated aggregation patterns in query logs. Measure bytes scanned before vs after rollups. |
| **Time-Series & Trend Metrics** | Trend queries run on pre-partitioned data. No self-joins on large fact tables. Window functions run over sorted partitions. | Self-joins on large tables for WoW/MoM calculations are among the most expensive patterns possible. | Check EXPLAIN for self-joins on large tables. Verify time-based filters use partition pruning. Measure cost of LAG/LEAD vs pre-computed delta columns. |
| **Slowly Changing Dimensions (SCD)** | Dimension changes captured with correct effective/expiry dates. No overlapping date ranges. Joins use surrogate keys not natural keys. | Without SCD handling, point-in-time historical queries return wrong results. | Verify surrogate keys in fact tables. Query for overlapping validity ranges. Measure dimension table size growth over time. |
| **High-Cardinality Dimensions (Bridge Tables)** | Bridge tables pre-built at write time. Joins produce no unexpected row explosion. | Exploding arrays at query time on large fact tables causes row multiplication — the result set grows by orders of magnitude. | Look for UNNEST/EXPLODE on large fact tables in query plans. Verify row counts before and after joins don't multiply unexpectedly. |
| **Access Pattern Alignment** | The most expensive queries run with maximum pruning and minimum data movement because the schema was designed for them. | Schema misaligned with access patterns means every query pays for data it doesn't need. | Analyze query logs for most frequent WHERE, JOIN ON, GROUP BY columns. Measure query latency distribution (p50, p95, p99). Check bytes scanned per query. |

---

## 2. Physical Storage & Layout

How data is physically organized on disk and in storage. These decisions are expensive to change after the fact.

| Sub-criteria | Ideal State | Why It Matters | How to Inspect |
|---|---|---|---|
| **Storage Format** | All analytical data in columnar format (Parquet/ORC). Open table format (Iceberg/Delta/Hudi) used where ACID or incremental reads are needed. Row formats only at ingestion boundary. | CSV/JSON scans read every column for every row. Columnar reads only the columns needed. The difference can be 10–100x in I/O. | Check file format in storage layer. Measure bytes scanned with EXPLAIN. Compare query cost before vs after format conversion. |
| **File Size** | Files uniformly sized around 128 MB. File count per partition matches cluster parallelism. File count grows predictably with data volume. | Files under ~10 MB cause excessive metadata reads and task scheduling overhead. Files over 1 GB reduce parallelism — only one task can process them. | `aws s3 ls --recursive` to inspect sizes. Check task count per partition in Spark UI. Count files per partition directory. |
| **Encoding & Compression** | Encoding chosen per-column based on data characteristics, not applied uniformly. Compression is absent on boolean columns and small tables. | Wrong encoding increases I/O overhead. Compressing booleans adds overhead with no storage benefit. Sort key encoding in Redshift disables zone map benefits. | Check storage size before/after encoding. Verify compression ratio per column in metadata. Measure I/O reduction on representative queries. |
| **Partitioning & Distribution Strategy** | Every query uses partition pruning to eliminate irrelevant data. Joins on distributed/bucketed keys require no data movement. | No partitioning = full table scan on every query. Wrong distribution key = every join shuffles all data across the network. | EXPLAIN for partition pruning evidence. Check partitions scanned vs total in query stats. Measure shuffle bytes in Spark UI for join queries. |
| **Data Types** | Each column uses the minimal type. Implicit casts are absent from all execution plans. | Implicit casts on join columns disable pushdown and force full scans. Oversized types waste storage and slow sort/hash operations. | Review DDL for oversized types (VARCHAR(MAX) where VARCHAR(50) suffices). Check EXPLAIN for implicit cast warnings. |
| **Clustering Structures & Indexes** | High-frequency filter columns have zone maps or equivalent structures allowing the engine to skip the majority of data blocks without reading them. | Without block skipping, the engine reads all data even when only 1% matches the filter. | Verify sort/cluster key is used in query plan. Measure block skipping effectiveness via scan reduction before/after. Check zone map hit rate. |
| **Table Statistics** | Statistics are current (updated after significant data changes), accurate, and cover all columns used in filters and joins. | Stale statistics cause poor join order, wrong join strategy selection, and bad memory estimates — all silently. The optimizer can't tell you its estimates are wrong. | Check last statistics update timestamp. Compare estimated vs actual row counts in EXPLAIN. A >10x discrepancy is a red flag. |
| **Metadata (Open Table Formats)** | Metadata is compact, snapshots are expired per policy, and column statistics are populated and used in query planning. | Metadata bloat from uncleaned snapshots degrades query planning performance and inflates storage costs — often invisibly. | Check snapshot count and metadata file size. Measure metadata size growth over time. Verify partition statistics are populated. |

---

## 3. Data Ingestion

How data enters the platform. The load pattern chosen here determines both cost and accuracy of downstream data.

| Sub-criteria | Ideal State | Why It Matters | How to Inspect |
|---|---|---|---|
| **Load Pattern (Bulk / Incremental / CDC)** | Load pattern matches change frequency. CDC or incremental for large frequently-changing tables. Bulk only for small or rarely-changed tables. | Full bulk reload of a 10 TB table every hour because it's "simpler" is a common anti-pattern. Incremental can be 100x cheaper for high-volume stable-schema tables. | Measure load duration vs data volume over time. Check for duplicate records after incremental loads. Verify no data gaps in watermark sequence. |
| **Source-Specific Considerations** | Schema changes are detected and propagated (not silently dropped). Late arrivals have a defined correction window. Source row counts are reconciled against loaded row counts. | Schema drift that silently drops a new source column can corrupt downstream models for days before detection. Late arrivals can cause permanent data gaps if not handled. | Monitor ingestion job failures and row count reconciliation. Track schema change events. Measure late arrival rate. Alert on source-to-target row count mismatch. |

---

## 4. Query & Compute Execution

How queries are planned and executed. The largest gains in query optimization come from here.

| Sub-criteria | Ideal State | Why It Matters | How to Inspect |
|---|---|---|---|
| **Data Reduction (Predicate / Partition / Projection Pushdown)** | Bytes scanned equals only what the query actually needs. Functions are never applied to filter columns. | A query that scans 10 TB when it needs 10 GB is paying 1000x too much in I/O. This is the single highest-leverage optimization. | EXPLAIN for pushdown evidence. Check "bytes scanned" in query results. Verify partition filter application in engine logs. Compare scanned data before vs after query rewrites. |
| **Join Strategy** | Broadcast Hash Join used wherever one side is small enough. No Nested Loop Joins in production plans. | A missed broadcast opportunity on a 100 MB dimension table touching a 1 TB fact table forces 1 TB of shuffle. NLJ without indexes = Cartesian product. | EXPLAIN for join type used. Monitor shuffle and spill metrics. Verify row count after join doesn't multiply unexpectedly. |
| **Data Movement & Shuffle** | Zero shuffle operations in the execution plan. Data is co-located at rest for frequent joins. | Shuffle = serialize data on node A, send over network, deserialize on node B. For large datasets this dominates job runtime. | Spark UI shuffle read/write bytes per stage. Redshift EXPLAIN for redistribution type. Network I/O metrics at cluster level. |
| **Data Skew** | All tasks complete in roughly equal time. Max task duration is less than 2x the median. | One straggler task holds up the entire stage. A 10-hour job completing in 9 hours and 59 minutes because one partition had 90% of the data is common. | Spark UI task duration variance per stage. Engine alert logs for skew warnings. Plot task duration histogram. Check max/min/avg partition size. |
| **Resource Allocation (Compute / Memory)** | No spills to disk. All sort and aggregation operations complete in memory. Cluster utilization is consistent without queuing. | Disk spill during sort or aggregation forces multi-pass algorithms. Multi-pass sort can be 10–100x slower than in-memory sort. | Spark UI executor memory metrics and spill indicators. Engine alert logs for memory warnings. Cloud provider dashboards for cluster-level utilization. |
| **Approximate Computation** | Approximate functions used where error tolerance permits. Exact distinct counts reserved only for billing or compliance. | `COUNT(DISTINCT user_id)` on billions of rows is expensive. HyperLogLog returns the same answer with ~1% error at orders-of-magnitude lower cost. | Compare approximate vs exact results to confirm error is within bounds. Measure query speedup. Document error tolerance in the data contract. |
| **SQL Construct Choices** | SQL constructs are chosen with knowledge of how the target engine parses them, not just semantic equivalence. | `UNION` vs `UNION ALL` differs by one deduplication step that may sort 100 GB of data. `CTE` inlined vs materialized changes whether work is repeated. Semantically equivalent ≠ cost equivalent. | Compare EXPLAIN plans for equivalent query rewrites. Measure actual execution time for DISTINCT vs GROUP BY, UNION vs UNION ALL. Check if CTE is inlined or materialized. |
| **Execution Plan Analysis** | Every production query has a reviewed EXPLAIN plan. Estimated rows are within 2x of actual. Plan review is part of the pipeline code review checklist. | The optimizer is invisible — it silently makes the wrong choice when statistics are stale. EXPLAIN makes the choice visible before it hits production. | Run EXPLAIN before deploying any non-trivial query. Compare estimated vs actual row counts. Verify expected join type appears. Confirm partition pruning is active. |

---

## 5. Precomputation & Reuse

Avoiding repeated computation is often cheaper than optimizing the computation itself.

| Sub-criteria | Ideal State | Why It Matters | How to Inspect |
|---|---|---|---|
| **Materialized Views** | Expensive, stable aggregations are materialized and auto-refreshed incrementally. Downstream queries transparently redirect to the view. | A dashboard query that re-aggregates 1 TB every hour is 1 TB × query_count × hours = unbounded cost. Materializing it once makes it fixed cost. | Track view refresh duration. Compare query latency with vs without materialized view. Monitor staleness (time since last refresh). |
| **Engine-Level Caching** | High-frequency dashboard queries achieve near-100% result cache hit rate. Cache invalidates correctly after data updates. | Result cache turns a 30-second query into a <100ms response for identical repeated requests — no computation at all. | Compare first run (cache miss) vs subsequent run (cache hit) latency. Monitor cache hit ratio. Verify cache invalidates after data changes. |
| **Pre-aggregated Tables** | No query scans the raw fact table for a metric that could have been pre-aggregated. Downstream consumers always query the smallest table meeting their granularity. | A team with 20 dashboards all independently scanning the raw fact table for daily totals does 20x the work needed. | Compare bytes scanned: raw fact table vs pre-aggregated table. Audit downstream query patterns for missed pre-aggregation opportunities. |

---

## 6. Operations & Lifecycle

Ongoing maintenance to keep performance from degrading silently over time.

| Sub-criteria | Ideal State | Why It Matters | How to Inspect |
|---|---|---|---|
| **Table Maintenance (Vacuum / Compaction / Stats)** | Maintenance runs automatically on a schedule. No table has dead row ratio >10%. File count is stable. Statistics are less than 24 hours old. | Redshift tables with high dead-row ratios scan extra blocks invisibly. S3 tables accumulating small files slow query start time by minutes. Both degrade silently. | Check dead row ratio. Count files per partition. Compare statistics age vs last data modification timestamp. |
| **Data Retention & Archival** | Retention policies are defined, automated, and enforced per table. Snapshot count is bounded. | Uncleaned Iceberg/Delta snapshots can bloat metadata 10–100x. Unbounded table growth raises storage cost and slows query planning. | Monitor partition storage size growth over time. Track snapshot count for open table format tables. Verify S3 Lifecycle policies are applied. |
| **Database-Level Operations** | Automated backups run on schedule. Restoration has been tested. Any bulk operation is preceded by a snapshot. | An untested backup is not a backup. Most teams discover their backup doesn't work during the incident. | Verify backup schedule and retention period. Test restore periodically. Validate row counts after bulk loads. |
| **Cluster & Infrastructure Operations** | Cluster utilization stays between 50–70%. Upgrade windows are pre-scheduled. Scaling is automated based on utilization thresholds. | Under-provisioning causes query queuing and missed SLAs. Over-provisioning wastes money. Unplanned upgrades during business hours cause incidents. | Monitor CPU/memory utilization trends. Track query queue depth over time. Review upgrade changelog for breaking changes before applying. |

---

## 7. Observability

If you can't measure it, you can't improve it — and you won't know when it regresses.

| Sub-criteria | Ideal State | Why It Matters | How to Inspect |
|---|---|---|---|
| **Resource & Query Metrics** | Metrics captured for every query. Dashboarded by team/user. Regressions detected before users report them. | Without metrics, performance problems are discovered by users, not engineers. Debugging without metrics means guessing. | Query engine system tables and history logs. Cloud monitoring dashboards for cluster-level metrics. Spark UI for job-level breakdown. |
| **Adoption & Throughput Metrics** | Query volume trends are visible. SLA compliance tracked per team or workgroup. Capacity scaling decisions driven by trend data, not incidents. | Query volume doubling without re-sizing causes latency to degrade gradually — impossible to notice without trending data. | Query system audit logs. Review WLM queue statistics. Track query success/failure rates. Plot query volume over time. |
| **Execution Plan Inspection** | Every production query has a reviewed EXPLAIN plan. Plan review is part of the code review checklist for data pipelines. | One unreviewed query with a Cartesian product silently consuming 100x the expected resources is a common production incident pattern. | Run EXPLAIN before deploying any non-trivial query. Compare estimated vs actual row counts. Verify expected join type appears. Confirm partition pruning is active. |
| **Alerts & Automation** | No human monitors dashboards for routine issues. All known failure modes have automated detection and at least a pager alert with a runbook link. Alerts fire on leading indicators, not lagging ones. | Alerting after SLA is breached is reactive. Alerting on queue depth rising or memory pressure building catches problems before users notice. | Verify alerts fire correctly in staging. Review alert fatigue — high false positive rate means alerts are ignored. Confirm each alert has a runbook. |
