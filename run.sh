#!/bin/bash
die() { echo $1 1>&2; exit 1; }
ENV=$1
[ -z $ENV ] && die "Usage: ./run.sh <env>"
./clean.sh $ENV
docker run --name "mesos-stats-$ENV" -it mesos-stats \
	"mesos2-$ENV.otenv.com:5050" \
	"carbon-$ENV.otenv.com" \
	2003 \
	"$ENV"
