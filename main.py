from fastapi import FastAPI
from routes.events import events_router
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from data_extraction import external_source_extraction_data
from data_transformation import data_cleaning_process, cronjob_log


app = FastAPI()

app.include_router(events_router)

scheduler = BackgroundScheduler(timezone='UTC')
scheduler.add_job(external_source_extraction_data, trigger=IntervalTrigger(hours=1, minutes=1))
scheduler.add_job(data_cleaning_process, trigger=IntervalTrigger(hours=1, minutes=1))
scheduler.add_job(cronjob_log, trigger=IntervalTrigger(minutes=1))
scheduler.start()