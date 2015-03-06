# Mesos Stats

Scripts to collect mesos cluster stats.

## mesos-stats-graphite.py

Usage:

```shell
shell> python mesos-stats-graphite.py <MASTER> <GRAPHITE_HOST> <GRAPHITE_PORT> <GRAPHITE_PREFIX>
```

Where:
- `<MASTER>` is the host/port combination of any mesos master in the cluster. The leader is automatically discovered
and queried for the slave stats, e.g. `my-mesos-master1.my-env.com`
- `<GRAPHITE_HOST>` is the host for your carbon instance, e.g. `carbon.host.com`
- `<GRAPHITE_PORT>` is the port for your carbon instance, e.g. `2003`
- `<GRAPHITE_PREFIX>` is a prefix for the stats, e.g. `mesos.some-env`
