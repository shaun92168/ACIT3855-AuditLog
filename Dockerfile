FROM ubuntu:16.04

MAINTAINER Shaun "shaun92168@gmail.com"

RUN apt-get update -y
RUN apt-get install -y python-software-properties
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update -y && \
    apt-get install -y python3.7 python3-pip

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

COPY . /app


ENTRYPOINT [ "python3" ]

CMD [ "app.py" ]
