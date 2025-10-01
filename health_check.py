#!/usr/bin/env python3

#########################################################################################
## To initialize your cluster setup based on the config file, run:                     ##
##     python scheduler.py init                                                        ##
#########################################################################################

import datetime
import os
import subprocess
import sys
import logging
import json
import time
from datetime import timedelta, datetime
import random
import http.client
from scheduler_core.services import Service, ServiceList, ServiceJob, service_dir, cluster_file, TIME_FORMAT, squeue_time_to_timedelta

##############################################################################
## Configuration                                                            ##
##############################################################################

# Get the current month and year
current_month = datetime.now().strftime("%Y-%m")

# Create the log filepath with the current month
log_filepath = os.path.expanduser(f"./log/scheduler-{current_month}.log")
config_file = "config.json"
squeue_path = '/usr/local/slurm/current/install/bin/squeue'
sbatch_path = '/usr/local/slurm/current/install/bin/sbatch'
scancel_path = '/usr/local/slurm/current/install/bin/scancel'
EXPIRE_LIMIT = "9:30"
FILE_LOCK = ".scheduler.lock"
FILE_LOCK_HEALTH = ".scheduler.health.lock"
MIN_PORT = 61001
MAX_PORT = 62000
ROUTINE_INTERVAL = 5     # Period of check_routine
ROUTINE_WINDOW = 60      # For moving average
LAST_INTERVAL_WEIGHT = 2 # Multiplier for number of active inferences in recent interval

def acquire_lock_health():
    # Set a lock file with the current process id, to serve as a mutex across multiple processes
    try:
        # Create the file (O_CREAT), fail if it exists (O_EXCL), open in read write mode (O_RDWR)
        fd = os.open(FILE_LOCK_HEALTH, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.write(fd, str.encode(str(os.getpid())))
        os.close(fd)
        logging.debug('Set lock for routine scheduling check.')
        return True
    except OSError:
        #with open(FILE_LOCK) as f:
        #    line = f.readline().strip()
        #    if len(line) == 0:
        #        return False
        #    pid = int(f.readline().strip())
        #if is_running(pid):
        logging.warning('Previous routine check is still running.')
        return False
        #logging.info('Previous routine check is not running, resetting lock.')
        #os.remove(FILE_LOCK)
        #return acquire_lock()


def release_lock_health():
    # Attempt to release the lock
    try:
        os.remove(FILE_LOCK_HEALTH)
        logging.debug('Released health lock.')
        return True
    except OSError as e:
        logging.error('Could not release health lock: ' + str(e))
        return False

def health_check():
    """Check the health of all active service jobs and cancel unhealthy jobs."""
    logging.debug('Performing health check on all active service jobs.')

    # Try to acquire the lock to ensure no other instance is running
    lock_acquired = False
    lock_acquired_health = False
    try:
        if not acquire_lock():
            logging.warning("Routine process is running. Exiting.")
            return
        lock_acquired = True

        # Read the service list from the cluster file
        with open(os.path.join(service_dir, cluster_file), 'r') as f:
            json_data = json.loads(f.read())

        service_list = ServiceList()
        service_list.from_json(json_data)
        squeue_output = get_squeue_status()
        service_list.update_service_job_from_queue(squeue_output)

        # Release routine lock, as health check can take some time
        release_lock()
        lock_acquired = False     

        # Lock health check to avoid duplicates
        if not acquire_lock_health():
            logging.warning("Another health check process is running. Exiting.")
            return
        lock_acquired_health = True
        logging.debug("Health check process starting...")

        # Iterate through all services and their jobs
        for service in service_list.services:
            for service_job in service.service_jobs:
                if service_job.ready and service_job.host and not service_job.is_about_to_expire():
                    # Check the health of the service job
                    logging.debug(f'Checking health for ready job {service_job.jobid} on {service_job.host}:{service_job.port}')
                    healthy = False
                    if service.health_check_script:
                        # custom script should exit 0 when healthy
                        try:
                            res = subprocess.run(
                                [service.health_check_script, f"{service_job.host}:{service_job.port}", f"{service.id}"],
                                capture_output=True, text=True, timeout=180
                            )
                            healthy = (res.returncode == 0)
                            logging.debug(f"Job {service.id} on {service_job.host}:{service_job.port} health is {healthy}")
                        except subprocess.TimeoutExpired as e:
                            logging.error(f"Health check for {service.id} failed due to timeout: {e}")
                            healthy = False  # explicitly mark as unhealthy
                        except Exception as e:
                            logging.info(f"Error during custom health check for {str(e)}, falling back to standard health check")
                            healthy = check_service_job_status(service_job.host, service_job.port)
                    else:
                        # fallback to built-in curl probe
                        healthy = check_service_job_status(service_job.host, service_job.port)

                    if healthy:
                        logging.debug(f'Job {service_job.jobid} on {service_job.host}:{service_job.port} is healthy.')
                    else:
                        logging.error(f'Job {service_job.jobid} on {service_job.host}:{service_job.port} is unhealthy. Cancelling.')
                        cancel_job(service_job.jobid)
                

        # Save updated service list back to the file
        # service_list.save_to_file()
        logging.debug('Health check completed.')

    except Exception as e:
        logging.error(f"Error during health check: {str(e)}")

    finally:
        if lock_acquired:
            release_lock()
        if lock_acquired_health:
            release_lock_health()

def main():
    logging.basicConfig(filename=log_filepath, level=logging.INFO,
                        format='%(asctime)s.%(msecs)d %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    return health_check()

if __name__ == '__main__':
    main()