
# import monkey  # TODO
# monkey.__package__
import sys,os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from celery import Celery
import conf
# from celery.loaders.app import AppLoader

app = Celery('site')
app.conf.broker_url = conf.CELERY_BROKER_URL
app.conf.result_backend = conf.CELERY_RESULT_BACKEND
# app.conf.beat_sync_every =  True
# app.config_from_object(conf, namespace='CELERY')

## 配置队列
# from kombu import Queue
# app.conf.task_default_queue = 'default'  
# app.conf.task_queues = (  
#     Queue('default', routing_key='default'),
#     Queue('wechat', routing_key='wechat'),
#     Queue('weather', routing_key='weather'),

# )

# Load task modules from all registered Django apps.
app.autodiscover_tasks()


@app.task(name="test_task")
def test_task():
    print(">> this is test task")