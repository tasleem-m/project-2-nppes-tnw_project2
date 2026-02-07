# Large-Scale Healthcare Data Pipeline
## Polars + S3 + Census API + dbt + DuckDB (10GB+ Processing)

A production-style healthcare data engineering pipeline that processes 10GB+ NPPES provider data using Polars (lazy execution), stages raw data in AWS S3, integrates live U.S. Census demographic data via API, and implements a dbt medallion architecture powered by DuckDB.
Built to demonstrate scalable data engineering patterns used in modern analytics stacks.

### Project Highlights

- Processed 10GB+ CSV without memory failures

- Implemented streaming + lazy evaluation with Polars

- Pulled live demographic data from the U.S. Census API

- Normalized and integrated API data with provider records

- Designed a medallion architecture (raw → staging → marts)

- Built analytical marts answering real healthcare business questions

- Implemented comprehensive dbt generic + custom data quality tests

- Integrated S3 data lake with DuckDB external sources

- Designed for incremental weekly updates and 10x scale growth

This project simulates a real-world healthcare analytics platform requiring scalability, performance, and data quality enforcement.

### Business Context

*Client: HealthAnalytics Platform*

The goal was to build a national healthcare intelligence system capable of:

- Handling massive NPPES provider datasets

- Integrating geographic and Census demographic data

- Identifying provider distribution gaps

- Supporting regulatory-grade data quality validation

### Architecture

S3 Data Lake (Raw Files)
        │
        ▼
Polars Lazy Ingestion Layer
(streaming, memory-efficient)
        │
        ▼
Census API Extraction Layer 
        │
        ▼
DuckDB (Analytical Engine)
        │
        ▼
dbt Medallion Architecture
raw → staging → marts
        │
        ▼
Tested, Analytics-Ready Models

### Why Polars?

Polars was chosen over Pandas for:

- Lazy execution model

- Query optimization

- Streaming engine support

- Lower memory footprint

- Faster performance on large datasets

### Data Organization (S3)
   
S3 Data Lake    
│
│
(Raw Staging)
│
│ ├─ nppes/
│ ├─ geographic/
│ ├─ reference/

- Raw data remains immutable

- Domain-based organization

- Supports incremental updates

### Medallion Architecture (dbt)

**Raw Layer**

- External sources pointing to S3 files

- No transformations

- Immutable source data

**Staging Layer**

- 1:1 cleaning models

- Data type standardization

- Null handling

- Geographic joins

- Census integration

**Marts Layer**

- Business-ready analytics models combining provider, geographic, and demographic data

### Challenges Solved

- Prevented memory overflow using Polars streaming engine

- Handled inconsistent data types in large CSV files

- Resolved null-heavy columns in regulatory data

- Designed referential integrity tests across multiple models

- Optimized DuckDB queries for large joins

### Example Business Insights

- Identified counties with low provider-to-population ratios

- Mapped specialty gaps by region

- Calculated provider density across states

- Highlighted potential underserved healthcare areas

### What This Demonstrates

This project showcases:

- Large-scale data processing

- Cloud data lake integration

- Modern analytics engineering practices

- dbt testing strategy

- Memory-efficient Python data engineering

- Production-style architecture design

- Documentation-first debugging approach
