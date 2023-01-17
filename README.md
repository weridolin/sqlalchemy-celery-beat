### 参考 [django-celery-beat](https://github.com/celery/django-celery-beat)实现的基于sqlalchemy的beat

### 仅供学习用



### issue todo
- 1.修改时马上触发tick,而不是等下次到达时间再触发tick
- a: 可以修改scheduler两次tick之间的间隔来实现. 因为修改完数据库的操作和scheduler的操作不是同个进程,暂时没法想到更好的方法来通知到scheduler



## 运行实例
- 1. `flask --app flask_test.apps run`启动flask,发送请求 `/create1` ,`/create2`, `/create3` 创建任务.
- 2. 修改conf.py,配置celery的相关配置
- 3. `celery -A flask_test beat -l DEBUG --scheduler schedulers:DatabaseScheduler`运行scheduler




