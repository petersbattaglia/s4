# Rebuilt on Sun May 11 10:06:34 PM PDT 2025

FROM ubuntu:20.04
MAINTAINER Peter Battaglia <me@petersbattaglia.com>

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -y update && apt-get -y upgrade
RUN apt-get install -y python python3-pip virtualenv gunicorn nano

# Setup flask application
RUN mkdir -p /deploy/app
COPY gunicorn_config.py /deploy/gunicorn_config.py
COPY src/requirements.txt /deploy/app/requirements.txt
RUN pip install -r /deploy/app/requirements.txt

COPY src /deploy/app

WORKDIR /deploy/app

EXPOSE 5000

# Start gunicorn
ENTRYPOINT ["/usr/bin/gunicorn", "--reload", "--config", "/deploy/gunicorn_config.py", "index:app"]