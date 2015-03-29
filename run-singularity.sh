#!/usr/bin/env bash
die() { echo $1 1>&2; exit 1; }
if ! command -v pip >/dev/null; then
	die "pip required"
fi
if [ -f requirements.txt ]; then
	pip install -r requirements.txt
fi
./mesos-stats-graphite.py $@
