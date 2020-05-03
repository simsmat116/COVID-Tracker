FROM ubuntu:18.04
MAINTAINER Matt Sims

RUN apt-get install -y python-pip

COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
