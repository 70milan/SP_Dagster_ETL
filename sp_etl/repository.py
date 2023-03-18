from dagster import repository

from sp_etl.jobs.run_etl import run_etl_job
from sp_etl.schedules.download_scheduler import etl_job_schedule
#from sp_etl.sensors.my_sensor import my_sensor
#etl job schedule from etl.schedules.etl_job_schedule import etl_job_schedule

@repository
def etl():
    """
    The repository definition for this etl Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [run_etl_job]
    schedules = [etl_job_schedule]    

    return jobs + schedules

'''


    jobs = [say_hello_job, run_etl_job]
    schedules = [my_hourly_schedule, etl_job_schedule]
    sensors = [my_sensor]

    return jobs + schedules + sensors


@repository
def etl():
    """
    The repository definition for this etl Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [say_hello_job]
    schedules = [my_job]
    sensors = [my_sensor]

    return jobs + schedules + sensors

'''