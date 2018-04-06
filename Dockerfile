FROM python:3.6.5-slim

MAINTAINER ssalisbury@opentable.com

# -- Install Pipenv:
RUN set -ex && pip install pipenv --upgrade

# -- Install Application into container:
RUN set -ex && mkdir /app
COPY . /app

WORKDIR /app

# -- Adding Pipfiles
ONBUILD COPY Pipfile Pipfile
ONBUILD COPY Pipfile.lock Pipfile.lock

# -- Install dependencies:
ONBUILD RUN set -ex && pipenv install --deploy --system

ENTRYPOINT ["python", "mesos_stats.py"]

# Note: When invoking this image, you need to pass some more args to docker run. See http://github.com/samsalisbury/mesos_stats readme for details.
