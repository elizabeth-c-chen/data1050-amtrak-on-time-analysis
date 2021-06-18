

from apscheduler.schedulers.background import BackgroundScheduler
from subprocess import call


sched = BackgroundScheduler(timezone='EST')


@sched.scheduled_job('interval', minutes=3)
def timed_job():
    call['python', 'test.py']


@sched.scheduled_job('cron', day_of_week='mon-sun', hour=8, minute=10)
def scheduled_job():
    call['python', 'auto_ETL.py']


sched.start()