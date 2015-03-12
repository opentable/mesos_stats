#!/bin/bash
docker run --name "test-mesos-stats" -it mesos-stats \
	master1.mesos-vpcqa.otenv.com:5050 \
	carbon-qa-uswest2.otenv.com \
	2003 \
	mesos.qa
