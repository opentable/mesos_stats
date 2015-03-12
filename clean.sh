#!/bin/bash
die() { echo $1 1>&2; exit 1; }
ENV=$1
[ -z $ENV ] && die "Usage: ./clean <env>"
name="mesos-stats-$ENV"
(
(docker ps -a | grep "$name") && docker rm -f "$name"
) >/dev/null
