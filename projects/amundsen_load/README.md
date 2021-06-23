# Amundsen Load of Data 

Amundsen requires a sequence of steps in order to properly surface metadata and information about tables to the front end.


## Steps

1. Ingest base data to Neo4J

2. Ingest additional data and decorate Neo4j over base data

3. Update Elasticsearch index

4. Remove stale data

## Processors

1. Ingest base data to Neo4J `ingest_base_data.py`
   - Prototype: Extract everything from DeltaLake
   - Need to check if there is extra config needed

2. Update Watermarks and Profile columns `taxi_watermark_job.py`
   - Added in watermarking process
   - Column Profiling using Pandas Profiling works
   - Need to adjust to run in bulk across tables
   - Not in main DAG yet
    
3. Update the Elasticsearch index `update_elasticsearch.py`
   - probably currently imports too many libraries

4. Remove stale data based on TTY `clean_stale_data.py`
   - Need to update to run across different schemas maybe?
   - Not in main DAG yet

## Requirements

-- All the Amundsen python requirements
-- SQLAlchemy
-- PyHive


```bash
#PyHive Install
# still have trouble with these
apt-get install libsasl2 libsasl2-dev libsasl2-modules

pip install 'pyhive[hive]'

```

## Building the Docker Operator

build docker from amundsen_load root

```bash


docker build -f docker/Dockerfile . -t amundsen_load 

```

```bash

# quick testing
docker run -it --env-file=common.env --network=datalake_ml_platform amundsen_load

```