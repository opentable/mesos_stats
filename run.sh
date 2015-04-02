#!/usr/bin/env bash
die() { echo $1 1>&2; exit 1; }
if ! command -v pip >/dev/null; then
	die "pip required"
fi
if ! (pip list | grep requests); then
	pip install requests
fi
"$@"
