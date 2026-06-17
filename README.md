# Performance Tuning Fundamentals

Time-tested principles for building and tuning data platforms — independent of vendor or technology. The fundamentals don't change; the vendor chapters show how each engine implements them.

## Structure

```
data-platform-fundamentals/
├── README.md                  ← you are here
├── 01-fundamentals.md         ← all principles and inspection methods (no vendor content)
├── 02-redshift.md             ← Amazon Redshift implementation guide
├── 03-pyspark-cradle.md       ← PySpark with Cradle implementation guide
├── 04-athena.md               ← Amazon Athena implementation guide
├── 05-glue.md                 ← AWS Glue implementation guide
└── summary.csv                ← full matrix, downloadable for spreadsheet use
```

## The 7 Criteria

| # | Criteria |
|---|---|---|
| 1 | [Data Modeling & Schema Design](./01-fundamentals.md#1-data-modeling--schema-design) |
| 2 | [Physical Storage & Layout](./01-fundamentals.md#2-physical-storage--layout) |
| 3 | [Data Ingestion](./01-fundamentals.md#3-data-ingestion) | 
| 4 | [Query & Compute Execution](./01-fundamentals.md#4-query--compute-execution) | 
| 5 | [Precomputation & Reuse](./01-fundamentals.md#5-precomputation--reuse) | 
| 6 | [Operations & Lifecycle](./01-fundamentals.md#6-operations--lifecycle) | 
| 7 | [Observability](./01-fundamentals.md#7-observability) |

**Total: 35 sub-criteria across 7 categories**

## How to Use This

**Learning a new technology?**
1. Read [01-fundamentals.md](./01-fundamentals.md) to know what questions to ask
2. Open the relevant engine chapter to see how that engine answers each question
3. Gaps in an engine chapter = open questions for you to research

**Tuning a slow query or job?**
1. Identify which category the problem falls into (data reduction? join strategy? skew?)
2. Read the principle to confirm ideal state
3. Jump to the engine chapter for specific commands, configs, and links

**Onboarding a new vendor (e.g., Snowflake, Trino, DuckDB)?**
1. Copy any engine chapter as a template
2. Work through each sub-criteria section and fill in the engine-specific details
3. Add a row to `summary.csv` for each sub-criteria

