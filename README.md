### 参考 [django-celery-beat](https://github.com/celery/django-celery-beat)实现的基于sqlalchemy的beat

### 仅供学习用



### TODO

- 1.solar实现

- 2.修改时马上触发tick,而不是等下次到达时间再触发tick
- a: 可以修改scheduler两次tick之间的间隔来实现. 因为修改完数据库的操作和scheduler的操作不是同个进程,暂时没法想到更好的方法来通知到scheduler

- 3.运行的路径问题,启动方式优化

- 4.clock tz问题



