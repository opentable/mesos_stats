import os
import sys
import time
import traceback
import queue
from datetime import datetime

from mesos_stats.util import log, Timer
from mesos_stats.mesos import (
    Mesos,
    MesosCarbon,
    MesosStatsException,
)
from mesos_stats.carbon import Carbon
from mesos_stats.singularity import Singularity, SingularityCarbon


def str_to_bool(s):
    if s == 'True':
        return True
    elif s == 'False':
        return False
    else:
        raise ValueError


def init_env():
    master_list = os.environ.get('MESOS_MASTER', '').split(',')
    carbon_host = os.environ.get('CARBON_HOST', None)
    graphite_prefix = os.environ.get('GRAPHITE_PREFIX', None)
    carbon_pickle = os.environ.get('CARBON_PICKLE', 'False')
    singularity_host = os.environ.get('SINGULARITY_HOST', None)
    carbon_port = os.environ.get('CARBON_PORT', '2003')
    dry_run = os.environ.get('DRY_RUN', 'False')

    dry_run = str_to_bool(dry_run)
    carbon_pickle = str_to_bool(carbon_pickle)

    def config_print():
        print("=" * 80)
        print("MESOS MASTERS:     %s" % master_list)
        print("CARBON:           %s" % carbon_host)
        print("GRAPHITE PREFIX:  %s" % graphite_prefix)
        print("CARBON PICKLE:  %s" % carbon_pickle)
        print("SINGULARITY HOST: %s" % singularity_host)
        print("DRY RUN (TEST MODE): %s" % dry_run)
        print("=" * 80)

    if not all([master_list, carbon_host, graphite_prefix]):
        print('ERROR : One or more configuration env not set')
        print('MESOS_MASTERS, CARBON, and GRAPHITE_PREFIX needs to be set')
        config_print()
        sys.exit(0)

    config_print()

    assert(isinstance(master_list, list))
    mesos = Mesos(master_list)
    carbon = Carbon(carbon_host, graphite_prefix, port=int(carbon_port),
                    pickle=carbon_pickle, dry_run=dry_run)

    if singularity_host:
        singularity = Singularity(singularity_host)

    return (mesos, carbon, singularity, carbon_pickle)


def wait_until_beginning_of_clock_minute():
    iter_start = time.time()
    now = datetime.fromtimestamp(iter_start)
    sleep_time = 60.0 - ((now.microsecond/1000000.0) + now.second)
    log("Sleeping for %ss" % sleep_time)
    time.sleep(sleep_time)


def main_loop(mesos, carbon, singularity, pickle):
    should_exit = False
    # self-monitoring
    assert all([mesos, carbon])  # Mesos and Carbon is mandatory

    metrics_queue = queue.Queue()
    if singularity:
        singularity_carbon = SingularityCarbon(singularity, metrics_queue,
                                               pickle)
        mesos_carbon = MesosCarbon(mesos, metrics_queue, singularity, pickle)
    else:
        mesos_carbon = MesosCarbon(mesos, metrics_queue, pickle=pickle)

    while True:
        try:
            wait_until_beginning_of_clock_minute()
            with Timer("Entire collect and send cycle"):
                timestamp = time.time()
                now = datetime.fromtimestamp(timestamp)
                log("Timestamp: %s (%s)" % (now, timestamp))
                cycle_timeout = timestamp + 59.0
                if singularity:
                    with Timer("Singularity metrics collection"):
                        singularity.reset()
                        singularity.update()
                        singularity_carbon.flush_all()
                if mesos:
                    with Timer("Mesos metrics collection"):
                        mesos.reset()
                        mesos.update()
                        mesos_carbon.flush_all()
                if not metrics_queue:
                    log("No stats this time; sleeping")
                    continue

                send_timeout = cycle_timeout - time.time()
                log("Sending stats (timeout %ss)" % send_timeout)
                with Timer("Sending stats to graphite"):
                    carbon.send_metrics(metrics_queue, send_timeout)
        except MesosStatsException as e:
            log("%s" % e)
        except RuntimeError as e:
            log("%s" % e)
            should_exit = True
            sys.exit(1)
            break
        except (KeyboardInterrupt, SystemExit):
            print("Bye!")
            should_exit = True
            sys.exit(0)
            break
        except Exception as e:
            traceback.print_exc()
            log("Unhandled exception: %s" % e)
        except object as o:
            traceback.print_exc()
            log("Unhandled exception object: %s" % o)
        except:
            traceback.print_exc()
            log("Unhandled unknown exception.")
        else:
            log("Metrics sent successfully.")
        finally:
            if should_exit:
                return


if __name__ == '__main__':
    (mesos, carbon, singularity, pickle) = init_env()
    start_time = time.time()
    print("Start time: %s" % datetime.fromtimestamp(start_time))
    try:
        main_loop(mesos, carbon, singularity, pickle)
    except (KeyboardInterrupt, SystemExit):
        print("Bye!")
        sys.exit(0)
