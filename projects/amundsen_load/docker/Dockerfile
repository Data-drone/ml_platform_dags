#FROM python:3.8-slim-buster
FROM openjdk:8u242-jre

## Could try for python 3.8 to line up with everything else?
RUN apt-get update && \
    apt install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev \
        libssl-dev libsqlite3-dev libreadline-dev libffi-dev tk-dev curl libbz2-dev liblzma-dev
    
WORKDIR /opt
RUN curl -O https://www.python.org/ftp/python/3.8.10/Python-3.8.10.tgz && \
    tar xzf Python-3.8.10.tgz && \
    cd Python-3.8.10 && \
    ./configure --enable-optimizations && \
    make altinstall

# lets manually install pip too
RUN curl -O https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python3.8 get-pip.py

#apt-get install -y python3-dev python3-pip

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

CMD ["/scripts/modules/load_env_run_script.sh", "/scripts/modules/ingest_delta_base_metadata.py"]
