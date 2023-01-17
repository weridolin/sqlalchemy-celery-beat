"""Beat Scheduler Implementation."""
from sqlalchemy.orm import scoped_session
from db import SessionFactory
from utils import NEVER_CHECK_TIMEOUT
from models import (
    PeriodicTask, PeriodicTasks, IntervalSchedule, CrontabSchedule,ClockedSchedule
)
from kombu.utils.json import dumps, loads
from kombu.utils.encoding import safe_str, safe_repr
from celery.utils.time import maybe_make_aware
from celery.utils.log import get_logger
from celery.beat import Scheduler, ScheduleEntry
from celery import schedules
from celery import current_app
from multiprocessing.util import Finalize
import time
import math
import logging
import datetime
from cmath import log
import os
import sys
from clocked import clocked
sys.path.append(os.path.dirname(__file__))


# from clockedschedule import clocked

# This scheduler must wake up more frequently than the
# regular of 5 minutes because it needs to take external
# changes to the schedule into account.
DEFAULT_MAX_INTERVAL = 60  # seconds

ADD_ENTRY_ERROR = """\
Cannot add entry %r to database schedule: %r. Contents: %r
"""

logger = get_logger(__name__)
debug, info, warning = logger.debug, logger.info, logger.warning


class ModelEntry(ScheduleEntry):
    # model-row 转换成 task对象
    """Scheduler entry taken from database row."""

    # schedule对应的 schedule model
    model_schedules = (
        (schedules.crontab, CrontabSchedule, 'crontab'),
        (schedules.schedule, IntervalSchedule, 'interval'),
        # (schedules.solar, SolarSchedule, 'solar'),
        (clocked, ClockedSchedule, 'clocked')
    )

    # 每一次调用后,该task row需要保存的字段
    save_fields = ['last_run_at', 'total_run_count', 'no_changes']

    def __init__(self, model, app=None, session=None):
        """ 
            Initialize the model entry.
            model为定义的periodic task表中的item
        """
        self.app = app or current_app._get_current_object()  # celery app
        self.name = model.name  # 任务名称
        self.task = model.task  # 任务对应的函数路径,例如 project.tasks.taskfunc
        self.session = session  # 该task绑定的session
        try:
            # 每个定时任务必须对应一个调度计划,每个调度计划可以对应多个定时任务
            self.schedule = model.schedule
        except AttributeError:
            logger.error(f'任务{self.name}对应的schedule被取消.将该任务状态设置为 enable...')
            self._disable(model)  # 把该定时任务设置为不可用状态
        try:
            # 加载定时任务的参数,json序列化再保存的
            self.args = loads(model.args or '[]')
            self.kwargs = loads(model.kwargs or '{}')
        except ValueError as exc:
            logger.error(f"任务{self.name}对应的参数虚化列失败.将该任务状态设置为 enable...")
            self._disable(model)

        self.options = {}
        # celery任务分发参数
        for option in ['queue', 'exchange', 'routing_key', 'priority']:
            value = getattr(model, option)
            if value is None:
                continue
            self.options[option] = value

        # 任务超时限制
        if getattr(model, 'expires_', None):
            self.options['expires'] = getattr(model, 'expires_')

        # celery分发任务请求对应的header
        self.options['headers'] = loads(model.headers or '{}')
        self.options['periodic_task_name'] = model.name

        # 任务运行的次数
        self.total_run_count = model.total_run_count
        # 对应的数据库记录
        self.model = model

        # 上次运行时间
        if not model.last_run_at:  # 上次运行时间
            model.last_run_at = self._default_now()

        self.last_run_at = model.last_run_at

    def _disable(self, model):
        # 将 定时任务设置为 enabled
        model.no_changes = True
        model.enabled = False
        self.save()

    def is_due(self):
        if not self.model.enabled:
            # 如果当前的任务为不可调度的状态，则隔5秒后再次触发 scheduler.tick 去检测
            # schedstate tuple(is_due,next)
            return schedules.schedstate(False, 5.0)

        # 当前时间小于任务的开始时间
        if self.model.start_time is not None:
            now = self._default_now()
            # 暂时只考虑本地时间 TODO UTC?

            if now < self.model.start_time:
                # The datetime is before the start date - don't run.
                # send a delay to retry on start_time
                delay = math.ceil(
                    (self.model.start_time - now).total_seconds()
                )
                return schedules.schedstate(False, delay)

        # 只运行一次
        if self.model.one_off and self.model.enabled and self.model.total_run_count > 0:
            # 停用该task,所有的状态初始化
            self.model.enabled = False
            self.model.total_run_count = 0
            self.model.no_changes = False
            # 运行完的task状态更新回数据库
            # todo 已经加载的record不用每次都马上commit? 
            self.save()
            return schedules.schedstate(False, NEVER_CHECK_TIMEOUT)
        # CAUTION: make_aware assumes settings.TIME_ZONE for naive datetimes,
        # while maybe_make_aware assumes utc for naive datetimes
        tz = self.app.timezone
        last_run_at_in_tz = maybe_make_aware(self.last_run_at).astimezone(tz)

        # 返回 tuple(当前是否应该运行,下次检测时间间隔)
        # self.schedule is module.schedule
        return self.schedule.is_due(last_run_at_in_tz)

    def _default_now(self):
        now = datetime.datetime.utcnow()
        return now

    def __next__(self):
        # 生成下次运行时对应的 modelEntry
        # sqlalchemy 默认开启事务
        self.model.last_run_at = self._default_now()
        self.model.total_run_count += 1
        # 防止去更新到[periodic_tasks]表,
        # [periodic_tasks]表只限于非scheduler下的更新才会去更新
        self.model.no_changes = True
        # self.save()
        return self.__class__(self.model, session=self.session)

    next = __next__  # for 2to3

    def save(self):
        # 保存当前的entry对应的model
        obj = self.session.query(type(self.model)).get(self.model.id)
        if obj:
            for field in self.save_fields:
                setattr(obj, field, getattr(self.model, field))
        self.session.commit()

    @classmethod
    def to_model_schedule(cls, session, schedule):
        for schedule_type, model_type, model_field in cls.model_schedules:
            schedule = schedules.maybe_schedule(schedule)
            if isinstance(schedule, schedule_type):
                model_schedule = model_type.from_schedule(session, schedule)
                return model_schedule, model_field
        raise ValueError(
            'Cannot convert schedule type {0!r} to model'.format(schedule))

    @classmethod
    def from_entry(cls, name, app=None, session=None, **entry):
        # with SessionFactory() as session:
        params = cls._unpack_fields(session=session, **entry)
        logger.info(f"update or create task from entry:{params}")
        crontab_schedule = params.pop("crontab", None)
        interval_schedule = params.pop("interval", None)
        task = session.query(PeriodicTask).filter_by(name=name).first()

        if task:
            for k, v in params.items():
                if hasattr(task, k):
                    setattr(task, k, v)
        else:
            if crontab_schedule:
                task = PeriodicTask(
                    crontab_id=crontab_schedule.id, name=name, **params)
            elif interval_schedule:
                task = PeriodicTask(
                    interval_id=interval_schedule.id, name=name, **params)
            session.add(task)
        session.commit()
        return cls(model=task, app=app, session=session)

    @classmethod
    def _unpack_fields(cls, session, schedule,
                       args=None, kwargs=None, relative=None, options=None,
                       **entry):
        model_schedule, model_field = cls.to_model_schedule(session, schedule)
        entry.update(
            {model_field: model_schedule},
            args=dumps(args or []),
            kwargs=dumps(kwargs or {}),
            **cls._unpack_options(**options or {})
        )
        return entry

    @classmethod
    def _unpack_options(cls, queue=None, exchange=None, routing_key=None,
                        priority=None, headers=None, expire_seconds=None,
                        **kwargs):
        return {
            'queue': queue,
            'exchange': exchange,
            'routing_key': routing_key,
            'priority': priority,
            'headers': dumps(headers or {}),
            'expire_seconds': expire_seconds,
        }

    def __repr__(self):
        return '<ModelEntry: {0} {1}(*{2}, **{3}) {4}>'.format(
            safe_str(self.name), self.task, safe_repr(self.args),
            safe_repr(self.kwargs), self.schedule,
        )


"""
    每个DatabaseScheduler实例会保存所有的tasks信息。每隔一定的时间间隔会调用sync方法去更新,sync方法中会去调用到schedule属性.并且会去调用对应的 
    schedule方法,schedule方法会根据scheduler属性和PeriodicTasks中的最后更新进行比较(参考model.py,如果task表/schedule表更新后,会去对应update
    periodictasks表的最后更新字段,即只要做了修改.periodictasks表的最后更新字段就会去更新,若发现已经更新,则Scheduler会去获取periodictask表中的所有enable的数据.
    update到_schedule dict.

    scheduler执行异步任务的过程是,启动一个service.service依次去调用scheduler.tick方法,tick方法是从tasks heap获取到最新的一个需要执行的任务.并执行.同时返回下次要执行
    tick的时间,即最早的下个任务的开始时间，如果大于0，则再同步下任务。
    
    即如果当前有任务再运行，则不去同步数据库中的任务，哪怕有更改(tick返回0).如果当前没任务，就不要去执行，#TODO 如果执行了一个很耗时的任务呢？？？

    # 如果中途更新的话,但是还没有达到下次tick的话,是不会去同步数据库中的任务的。

"""


class DatabaseScheduler(Scheduler):
    """Database-backed Beat Scheduler."""

    Entry = ModelEntry
    Model = PeriodicTask
    Changes = PeriodicTasks

    _schedule = None
    _last_timestamp = None
    _initial_read = True
    _heap_invalidated = False

    # sync_at_once = True

    # session = scoped_session(session_factory=SessionFactory)

    def __init__(self, *args, **kwargs):
        """Initialize the database scheduler."""
        self._dirty = set()

        self.session = scoped_session(session_factory=SessionFactory)
        # @https://docs.sqlalchemy.org/en/14/errors.html#error-bhk3
        # 不自动刷新,根据配置来进行刷新.参考  should_sync()/sync()
        self.session.autoflush=False

        Scheduler.__init__(self, *args, **kwargs)
        self._finalize = Finalize(self, self.sync, exitpriority=5)
        self.max_interval = (
            kwargs.get('max_interval')
            or self.app.conf.beat_max_loop_interval
            or DEFAULT_MAX_INTERVAL)

    def setup_schedule(self):
        # TODO config scheduler in config file
        logger.info(
            "dataBase Scheduler setup up,load config schedule and syc to dataBase")
        self.install_default_entries(self.schedule)
        self.update_from_dict(self.app.conf.beat_schedule)

    def all_as_schedule(self):
        debug('DatabaseScheduler: Fetching database schedule')
        s = {}
        tasks = self.session.query(PeriodicTask).filter_by(enabled=True).all()
        for model in tasks:
            try:
                s[model.name] = self.Entry(
                    model, app=self.app, session=self.session)
            except ValueError:
                pass
        logger.info(
            f"get all task from periodic table and sync to scheduler:{s.values()}")
        return s

    def schedule_changed(self):
        try:
            # 检测任务是否有修改
            # logger.info("check last change time")
            last, ts = self._last_timestamp, self.Changes.last_change(
                session=self.session)
        except Exception as exc:
            print(exc)
            warning(
                'DatabaseScheduler: InterfaceError in schedule_changed(), '
                'waiting to retry in next call...'
            )
            return False
        try:
            if ts and ts > (last if last else ts):
                return True
        finally:
            self._last_timestamp = ts
        return False

    def reserve(self, entry):
        new_entry = next(entry)
        # Need to store entry by name, because the entry may change
        # in the mean time.
        self._dirty.add(new_entry.name)
        return new_entry

    def should_sync(self):
        # 同步运行中的tasks的状态到数据库中的条件:
        # 1.第一次运行:(_last_sync为None)
        # 2.当前时间距离上次同步时间小于设置的同步时间 sync_every
        # 3.待同步的任务数量(_tasks_since_sync)大于设置的最小数量(sync_every_tasks
        return (not self._last_sync or
                (time.monotonic() - self._last_sync) > self.sync_every) or \
            (self.sync_every_tasks and
                self._tasks_since_sync >= self.sync_every_tasks)       

    def sync(self):
        # 如果task在运行过程中被修改了(比如运行次数,上次运行时间),则调用modelEntity.save()同步到数据库
        logger.info("sys task ing ....")
        _tried = set()
        _failed = set()
        try:
            while self._dirty:
                name = self._dirty.pop()
                try:
                    logger.info(f"sync task:{name} to database")
                    self.schedule[name].save()  # modelEntity.save()
                    _tried.add(name)
                except (KeyError):
                    _failed.add(name)
        except Exception as exc:
            logger.error(f"sync tasks error:{exc}", exc_info=True)
        finally:
            # retry later, only for the failed ones
            self._dirty |= _failed

    def update_from_dict(self, mapping):
        s = {}
        for name, entry_fields in mapping.items():
            try:
                entry = self.Entry.from_entry(
                    name, app=self.app, session=self.session, **entry_fields)
                if entry.model.enabled:
                    s[name] = entry
            except Exception as exc:
                logger.exception(ADD_ENTRY_ERROR, name, exc, entry_fields)
        self.schedule.update(s)

    def install_default_entries(self, data):
        # 添加默认的 celery.backend_cleanup任务,只要启动就添加
        entries = {}
        # if self.app.conf.result_expires:
        #     # 定时清理后台任务
        #     entries.setdefault(
        #         'celery.backend_cleanup', {
        #             'task': 'celery.backend_cleanup',
        #             'schedule': schedules.crontab('0', '4', '*'),
        #             'options': {'expire_seconds': 12 * 3600},
        #         },
        #     )
        self.update_from_dict(entries)  # 写入task到数据库

    def schedules_equal(self, *args, **kwargs):
        if self._heap_invalidated:
            self._heap_invalidated = False
            return False
        return super().schedules_equal(*args, **kwargs)

    # 存放tasks的优先队列
    @property
    def schedule(self):
        initial = update = False
        if self._initial_read:
            debug('DatabaseScheduler: initial read')
            initial = update = True
            self._initial_read = False
        elif self.schedule_changed():
            info('DatabaseScheduler: Schedule changed.')
            update = True

        if update:
            info(f"scheduler change,begin to sync")
            self.sync()
            self._schedule = self.all_as_schedule()  # {model.name:modelEntity}
            # the schedule changed, invalidate the heap in Scheduler.tick
            if not initial:
                self._heap = []
                self._heap_invalidated = True
            if logger.isEnabledFor(logging.DEBUG):
                debug('Current schedule:\n%s', '\n'.join(
                    repr(entry) for entry in self._schedule.values()),
                )
        return self._schedule
