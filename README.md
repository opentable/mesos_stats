# Mesos Stats

Scripts to collect mesos cluster stats.

## Installation:
```shell
# Clone this repo, cd into the directory, and do:
shell> pip install . 
```

## Usage:
```shell
shell> mesos_stats <MASTER_HOST> <GRAPHITE_HOST> <GRAPHITE_PREFIX> [<SLEEP_SECONDS=60>]
```

Where:
- `<MASTER>` is the host of any mesos master in the cluster.
- `<GRAPHITE_HOST>` is the host for your carbon instance, e.g. `carbon.host.com`
- `<GRAPHITE_PREFIX>` is a prefix for the stats, e.g. `mesos.production.eu`
- `<SLEEP_SECONDS>` is the number of seconds to sleep between gathering date (default 60)

