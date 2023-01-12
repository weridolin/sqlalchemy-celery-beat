from flask import Flask
import sys,os
import threading
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from models import IntervalSchedule,PeriodicTask
import json
from db import SessionFactory


app = Flask("test_celery")


from sqlalchemy import select
@app.route("/create")
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


@app.route("/update")
def update_task():
    with SessionFactory() as session:
        record = session.query(PeriodicTask).filter_by(name='TestTask1-232').first()
        record.task = 'task.test_task_update'
        record.enabled=True
        record.one_off=False
        # session.add(tasks)
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