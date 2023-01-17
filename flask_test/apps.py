from flask import Flask
import sys,os
import threading
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from models import IntervalSchedule,PeriodicTask,CrontabSchedule,ClockedSchedule
import json
from db import SessionFactory


app = Flask("test_celery")


@app.route("/create1")
def create_task():
    with SessionFactory() as session:
        # session.expire_on_commit=False
        schedule = session.query(IntervalSchedule).filter_by(every=10,period=IntervalSchedule.SECONDS).first()
        if not schedule:
            schedule = IntervalSchedule(
                every=10,period=IntervalSchedule.SECONDS
            )
            session.add(schedule) 
            session.flush()

        task = session.query(PeriodicTask).filter_by(name='TestTask1-232').first()
        if not task:
            task=PeriodicTask(
                interval_id=schedule.id,      
                name='TestTask1-232',        
                task='task.test_task',  
                args=json.dumps([8]),
            )
            session.add(task)
        session.commit()
    return "ok"

@app.route("/create2")
def create_crontab_task():
    with SessionFactory() as session:
        # session.expire_on_commit=False
        schedule = session.query(CrontabSchedule).filter_by(
            minute="0-23/1",
            hour="*").first()
        if not schedule:
            schedule = CrontabSchedule(
                minute="0-23/1",
            )
            session.add(schedule) 
            session.flush()

        task = session.query(PeriodicTask).filter_by(name='TestTask-crontab').first()
        if not task:
            task=PeriodicTask(
                crontab_id=schedule.id,      
                name='TestTask-crontab',        
                task='task.test_task2',  
                args=json.dumps([8]),
            )
            session.add(task)
        session.commit()
    return "ok"

from utils import to_utc
@app.route("/create3")
def create_clocked_task():
    with SessionFactory() as session:
        # session.expire_on_commit=False
        import datetime
        clocked_time = datetime.datetime.now()+datetime.timedelta(seconds=2*60)
        # clocked_time = datetime.datetime.strftime(clocked_time,format="YYYY-MM-DD hh:mm:ss")
        schedule = session.query(ClockedSchedule).filter_by().first()
        if not schedule:
            schedule = ClockedSchedule(
                clocked_time=clocked_time,
            )
            session.add(schedule) 
            session.flush()

        task = session.query(PeriodicTask).filter_by(name='TestTask-clockedtask').first()
        if not task:
            task=PeriodicTask(
                clocked_id=schedule.id,      
                name='TestTask-clockedtask',        
                task='task.test_task2',  
                args=json.dumps([8]),
            )
            session.add(task)
        session.commit()
    return "ok"



import time
@app.route("/update")
def update_task():
    # with SessionFactory() as session:
    session =  SessionFactory()
    record = session.query(PeriodicTask).filter_by(name='TestTask1-232').first()
    record.task = 'task.test_task_update'
    record.enabled=True
    record.one_off=False
    record.no_changes =False
    record.total_run_count+=1
    # session.add(tasks)
    # print(record.no_changes)
    session.flush()
    time.sleep(100)
    session.commit()

    return "ok"

@app.route("/delete")
def delete_task():
    session = SessionFactory()
    task = session.query(PeriodicTask).get(2)
    if task:
        session.delete(task)
        session.commit()

    return "ok"


def test_task(count):
    print(">>>>>>",count)


import  time
if __name__=="__main__":
    ...