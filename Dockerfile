FROM python:2.7.9-slim

RUN pip install requests
COPY ./mesos-stats-graphite.py mesos-stats-graphite.py

ENTRYPOINT ["./mesos-stats-graphite.py"] 
