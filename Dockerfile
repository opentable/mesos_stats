FROM python:2.7.9-slim

MAINTAINER ssalisbury@opentable.com

ADD . /source
WORKDIR /source
RUN pip install .

ENTRYPOINT ["mesos_stats"]

