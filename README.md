# Advanced Stock Pipeline

A modern end-to-end stock data platform built with a lakehouse architecture.

## Overview

This project automates the ingestion, storage, transformation, and analysis of stock market data using modern data engineering tools.

## Tech Stack

* Apache Airflow – Workflow orchestration
* MinIO – S3-compatible object storage
* PostgreSQL – Metadata and analytics database
* Apache Spark – Distributed data processing
* Apache Iceberg – Table format for the lakehouse
* Project Nessie – Data catalog and version control
* Docker – Containerized deployment

## Architecture

1. Extract stock market data from external APIs.
2. Store raw data in MinIO.
3. Process and transform data using Spark.
4. Manage Iceberg tables through Nessie.
5. Serve curated datasets for analytics and machine learning.

## Getting Started

```bash
docker compose up -d
```

Access services:

* Airflow: http://localhost:8080
* MinIO Console: http://localhost:9001

## Project Status

🚧 Currently under active development.
