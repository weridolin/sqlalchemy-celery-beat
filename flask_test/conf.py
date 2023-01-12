import environ,os
environ.Env.read_env(os.path.join(os.path.dirname(__file__),".env"))
env = environ.Env()

CELERY_BROKER_URL =  f"redis://:werido@8.131.78.84:6379/1"
CELERY_RESULT_BACKEND = CELERY_BROKER_URL
#: Only add pickle to this list if your broker is secured
#: from unwanted access (see userguide/security.html)
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
