# Airflow DAGs for ML Platform 

A DAG Monorepo for my use with my ML Platform project
https://github.com/Data-drone/a_ml_platform

## taxi_load

Uses boto3 and minio to load raw files from AWS Public Dataset to Minio
Uses Spark to load data into a Delta Lake

Quirks:
- Airflow DAG folder can have subprojects
- Resolve pyspark scripts with full path to prevent issues (still unsure how it path resolves with relative paths)

Restructure plans:

- Multiple Workers, load raw in parallel?
- Make Delta ingest run in parallel with one per known schema
