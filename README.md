# Flights Data ETL Pipeline & Lakehouse

This project ingests flights and airport data from **Aerodatabox API** and **OurAirports CSV**, processes it into a Delta Lakehouse, and finally loads it into a **PostgreSQL staging database**. The final transformations are done with **dbt** to produce aggregated **Gold tables** for analysis. The entire workflow is automated with **Airflow** and containerized via **Docker**.

---

## Core Idea & Architecture

The main goal is to build a **reliable, automated data platform** that transforms heterogeneous sources into clean, standardized, and aggregated datasets, ready for analysis.

### Layers

1. **Bronze Layer:**  
   - Aerodatabox mini-batch API data is ingested into **raw Delta tables**.  
   - Basic schema applied to raw JSON; performance optimized for downstream processes.  

2. **Silver Layer:**  
   - Merge all sources into a **single source of truth (SSOT)**.  
   - OurAirports CSV is loaded here and merged with Aerodatabox data.  
   - Cleans, flattens, standardizes, and creates atomic Delta tables.  

3. **Staging Database:**  
   - Silver Delta tables are loaded into **PostgreSQL** using a pre-defined schema (managed by **Alembic**).  
   - Provides a structured, queryable staging environment for dbt transformations.  

4. **Gold Layer:**  
   - dbt transforms staging tables into **final analytical tables**.  
   - Aggregations, derived metrics, and business-ready views are created.  

---

## Key Technologies

* **Apache Spark & Delta Lake:** For batch processing and reliable Delta Lakehouse storage.  
* **PostgreSQL:** Staging relational database for structured storage and dbt transformations.  
* **dbt:** For transforming staging tables into final Gold tables.  
* **Airflow:** Orchestration of the full ETL pipeline.  
* **Docker & Docker Compose:** Containerization of all services (Spark, Postgres, Airflow).  
* **Python:** For ETL logic, API crawling, and utility scripts.  

---

## Setup & Instructions

### 1. Prerequisites

Before you begin, ensure you have the following installed on your system:

* **Docker & Docker Compose** – For containerizing Spark, Postgres, and Airflow.  
* **Python 3.10+** – For ETL scripts and utilities.  
* **Airflow** – Managed via Docker Compose.  
* **PostgreSQL** – Used as the staging database.  

---

### 2. Initial Setup

Follow these steps to configure the project on your local machine.

1. **Clone the Repository**

```bash
git clone github.com/yourusername/flights-data
cd flights-data
```

2. **Configure Environment Variables**

Copy the template file to create your local configuration, then edit paths and credentials as needed.

```bash
cp .env.example .env
nano .env
```

3. **Download OurAirports Data**

You need the CSV files from OurAirports. Download them from:

[https://github.com/davidmegginson/ourairports-data](https://github.com/davidmegginson/ourairports-data)

Copy the following files into the project `csv/` folder:

* `airports.csv`
* `countries.csv`
* `regions.csv`
* `runways.csv`

> This is a one-time download.

4. **Start Services**

Launch all necessary containers from the project root:

```bash
docker-compose up -d
```

5. **Initialize PostgreSQL Schema**

Run Alembic migrations **once** to create the staging database schema:

```bash
docker-compose run --rm alembic-run alembic upgrade head
```

6. **Run the ETL Pipeline via Airflow**

Airflow orchestrates the full workflow. Access the Airflow UI to trigger DAGs or monitor execution:

```
http://localhost:8081
```

From the UI, run pipeline_run with this configuration JSON:
```json
{
    "process_detailed_dims": true,
    "skip_crawl": true,
    "run_aerodatabox": true,
    "run_ourairports": true
}
```

Subsequent runs will be automated by Airflow

## Future Implementation

Planned enhancements for this project include:

* **Migration to a Cloud Data Warehouse:** Move the staging and/or Gold layer to a cloud platform such as **Snowflake** or **AWS Redshift** for better scalability, performance, and centralized data access.  
* **BI Tool Integration:** Connect the Gold tables to business intelligence tools (e.g., **Tableau**, **Looker**, **Power BI**) to enable dashboards, reports, and self-service analytics for end-users.  
