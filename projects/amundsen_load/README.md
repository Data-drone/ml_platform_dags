# Amundsen Load of Data 

Amundsen requires a sequence of steps in order to properly surface metadata and information about tables to the front end.


## Steps

1. Ingest base data to Neo4J

2. Ingest additional data and decorate Neo4j over base data

3. Update Elasticsearch index

4. Remove stale data

## Processors

1. Ingest base data to Neo4J

- Prototype: Extract everything from DeltaLake
- Need to check if there is extra config needed

2. Not implemented yet
    

3. Update the Elasticsearch index
   - probably currently imports too many libraries

4. Not implemented yet

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


docker build -f docker/Dockerfile -t amundsen_load 

```

```bash

# quick testing
docker run -it --env-file=common.env amundsen_load

```