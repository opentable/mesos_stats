# Mesos Stats
Scripts to collect Mesos and Singularity cluster stats.

## Usage:
Build and tag the docker image. E.g.

```
shell> docker build -t mesos_stats .
```

Create a Dockerfile inheriting from that image, and add some configuration, e.g.
```
FROM mesos_stats
ENTRYPOINT ["mesos_stats", "<MASTER>", "<CARBON_HOST>", "<GRAPHITE_PREFIX>", "<SINGULARITY_HOSTNAME>"]
```

Where:
- `<MASTER>` is the host of any mesos master in the cluster.
- `<CARBON_HOST>` is the host for your carbon instance, e.g. `carbon.host.com`
- `<GRAPHITE_PREFIX>` is a prefix for the stats, e.g. `mesos.production.eu`
- `<SINGULARITY_HOSTNAME>` is the hostname of your Singularity web interface (optional)


Then launch it on your cluster! The singularity host is optional, but adds extra stats.

