from dagster import schedule
from sp_etl.jobs.run_etl import run_etl_job


@schedule(cron_schedule="35 22 17 * *", job=run_etl_job, execution_timezone="US/Central")
def etl_job_schedule(_context):
    run_config = {}
    return run_config
