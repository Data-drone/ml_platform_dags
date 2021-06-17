#FROM python:3.8-slim-buster
FROM openjdk:8u242-jre

## Could try for python 3.8 to line up with everything else?
RUN apt-get update && \
    apt-get install -y python3-dev python3-pip

# update files
RUN apt-get update && \
    apt-get install -y netcat && \
    apt-get install -y vim libsasl2-dev libsasl2-modules

COPY docker/requirements.txt requirements.txt

# Load up the docker env flags
COPY common.env /scripts/common.env

RUN pip3 install -r requirements.txt

RUN mkdir -p /scripts

WORKDIR /scripts

ADD modules /scripts/modules
RUN chmod u+x /scripts/modules/load_env_run_script.sh

CMD ["/scripts/modules/load_env_run_script.sh", "/scripts/modules/ingest_base_data.py"]
