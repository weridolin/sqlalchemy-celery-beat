# -*- coding: utf-8 -*-
import datetime
import time
from sqlalchemy import event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker,scoped_session,relationship,validates
import os
import sqlalchemy as sa
from celery import schedules, current_app
from validators import hour_validator,minute_validator,day_of_month_validator,day_of_week_validator,month_of_year_validator
from datetime import timedelta
from celery.utils.log import get_logger


logger = get_logger(__name__)

class Base(object):

    id = sa.Column(sa.Integer, primary_key=True)

    created = sa.Column(
        sa.DateTime,
        default=datetime.datetime.now,
        nullable=False
    )
    last_update = sa.Column(
        sa.DateTime,
        default=datetime.datetime.now,
        onupdate=datetime.datetime.now,
        nullable=False
    )

DeclarativeBase = declarative_base(cls=Base)

class CrontabSchedule(DeclarativeBase):
    """
        Timezone Aware Crontab-like schedule.

        @https://cron.qqe2.com/

        crontab:
            *    *    *    *   *
            |    |    |    |   |____星期(0-7)
            |    |    |    |________月份1--12
            |    |    |_____________日期1--31
            |    |__________________小时0--23
            |_______________________分钟0--59
        
        *:表示所有
        a-b:表示 a到b
        a,b:表示a和b
        a-b/c:表示a-b中每隔c

        例1: 每天晚上23点到第二天7点之间，每隔一小时执行一次:  * 23-7/1 * * *
        例2: 在指定的月份执行一次（在1月,4月和 6月每天晚上0点执行一次）: 0 0 * jan,apr,jun *
        例3: 每天18 : 00至23 : 00之间每隔30分钟执行一次: 0,30 18-23 * * *


    """
    __tablename__ = "crontab_schedule"

    minute = sa.Column(
        sa.String(length=60 * 4),
        default='*',
        comment='分钟数. Use "*" for "all". (Example: "0,30")'
    )

    hour = sa.Column(
        sa.String(length=24 * 4),
        default='*',
        comment='小时数. Use "*" for "all". (Example: "8,20")',
        # validators=[validators.hour_validator],
    )
    day_of_week = sa.Column(
        sa.String(length=64),
        default='*',
        comment='日期. Use "*" for "all". (Example: "0,5")',
        # validators=[validators.day_of_week_validator],
    )
    day_of_month = sa.Column(
        sa.String(length=31 * 4),
        default='*',
        comment='月份. Use "*" for "all". (Example: "1,15")',
        # validators=[validators.day_of_month_validator],
    )
    month_of_year = sa.Column(
        sa.String(length=64),
        default='*',
        comment='年份. Use "*" for "all". (Example: "0,6")',
        # validators=[validators.month_of_year_validator],
    )

    # timezone = timezone_field.TimeZoneField(
    #     default=crontab_schedule_celery_timezone,
    #     use_pytz=False,
    #     comment='Timezone to Run the Cron Schedule on. Default is UTC.',
    # )


    # def __str__(self):
    #     return '{0} {1} {2} {3} {4} (m/h/dM/MY/d) {5}'.format(
    #         cronexp(self.minute), cronexp(self.hour),
    #         cronexp(self.day_of_month), cronexp(self.month_of_year),
    #         cronexp(self.day_of_week), str(self.timezone)
    #     )

    @property
    def schedule(self):
        crontab = schedules.crontab(
            minute=self.minute,
            hour=self.hour,
            day_of_week=self.day_of_week,
            day_of_month=self.day_of_month,
            month_of_year=self.month_of_year,
        )

        return crontab

    @classmethod
    def from_schedule(cls, session,schedule):
        ## schedule: celery.scheduler.crontab
        spec = {'minute': schedule._orig_minute,
                'hour': schedule._orig_hour,
                'day_of_week': schedule._orig_day_of_week,
                'day_of_month': schedule._orig_day_of_month,
                'month_of_year': schedule._orig_month_of_year,
                # 'timezone': schedule.tz
                }
        try:
            record = session.query(cls).filter_by(**spec).first()
            if record:
                return record
            else:
                new  = cls(**spec)
                session.add(new)
                return new
        finally:
            session.commit()

    @validates("minute")
    def validate_minute(self,key,value):
        return minute_validator(value=value)

    @validates("hour")
    def validate_hour(self,key,value):
        return hour_validator(value=value)

    @validates("day_of_week")
    def validate_day_of_week(self,key,value):
        return day_of_week_validator(value=value)

    @validates("day_of_month")
    def validate_day_of_month(self,key,value):
        return day_of_month_validator(value=value)

    @validates("month_of_year")
    def validate_month_of_year(self,key,value):
        return month_of_year_validator(value=value)


DAYS = 'days'
HOURS = 'hours'
MINUTES = 'minutes'
SECONDS = 'seconds'
MICROSECONDS = 'microseconds'

PERIOD_CHOICES = (
    (DAYS, 'Days'),
    (HOURS, 'Hours'),
    (MINUTES, 'Minutes'),
    (SECONDS, 'Seconds'),
    (MICROSECONDS, 'Microseconds'),
)

SINGULAR_PERIODS = (
    (DAYS, 'Day'),
    (HOURS, 'Hour'),
    (MINUTES, 'Minute'),
    (SECONDS, 'Second'),
    (MICROSECONDS, 'Microsecond'),
)

class IntervalSchedule(DeclarativeBase):
    """
        每个多少秒发送一次任务.定时周期支持 /days/hours/minutes/seconds/microseconds

        例1: 每隔10秒发送一次: every=10, period=SECONDS
        例2: 每隔10分钟发送一次: every=10, period=MINUTES
    >>> every=2, period=DAYS
    """

    __tablename__ = "interval_schedule"

    DAYS = DAYS
    HOURS = HOURS
    MINUTES = MINUTES
    SECONDS = SECONDS
    MICROSECONDS = MICROSECONDS

    PERIOD_CHOICES = PERIOD_CHOICES

    every = sa.Column(
        sa.Integer,
        nullable=False,
        comment='在每个周期内循环隔多少秒就发送一次任务',
    )
    period = sa.Column(
        sa.String(24),
        comment='循环周期,每天/分钟/小时/秒/微秒',
    )


    @validates('every')
    def validate_every(self, key, every):
        if every < 1:
            logger.debug("min interval is 1 seconds")
            every = 1
        return every

    @property
    def schedule(self):
        return schedules.schedule(
            timedelta(**{self.period: self.every}),
            nowfun=lambda: datetime.datetime.utcnow()
        )

    @classmethod
    def from_schedule(cls, schedule, period=SECONDS):
        with SessionFactory() as session:
            every = max(schedule.run_every.total_seconds(), 0)
            record = session.query(cls).filter_by(every=every, period=period).first()
            if record:
                return record
            else:
                return cls(every=every, period=period)


    def __str__(self):
        readable_period = None
        if self.every == 1:
            for period, _readable_period in SINGULAR_PERIODS:
                if period == self.period:
                    readable_period = _readable_period.lower()
                    break
            return 'every {}'.format(readable_period)
        for period, _readable_period in PERIOD_CHOICES:
            if period == self.period:
                readable_period = _readable_period.lower()
                break
        return 'every {} {}'.format(self.every, readable_period)

    @property
    def period_singular(self):
        return self.period[:-1]


class PeriodicTasks(DeclarativeBase):
    """Helper table for tracking updates to periodic tasks.

    This stores a single row with ``ident=1``. ``last_update`` is updated via
    signals whenever anything changes in the :class:`~.PeriodicTask` model.
    Basically this acts like a DB data audit trigger.
    Doing this so we also track deletions, and not just insert/update.

    """

    __tablename__ = "periodic_tasks"

    @classmethod
    def last_change(cls,session):
        record = session.query(cls).get(1)
        if record:
            return record.last_update
        else:
            new = cls()
            session.add(new)
            session.commit()
            return new.last_update

    

class PeriodicTask(DeclarativeBase):
    """
        定时任务    
    """

    __tablename__ = "periodic_task"

    name = sa.Column(
        sa.String(256), 
        unique=True,
        comment='定时任务名称',
    )
    task = sa.Column(   
        sa.String(256),
        comment='定时任务的执行路径(例如:"proj.tasks.import_contacts")',
    )

    interval_id = sa.Column(sa.INTEGER,sa.ForeignKey("interval_schedule.id"))
    interval = relationship("IntervalSchedule")

    crontab_id = sa.Column(sa.INTEGER,sa.ForeignKey("crontab_schedule.id"))
    crontab = relationship("CrontabSchedule")

    # solar_id = sa.Column(sa.INTEGER,sa.ForeignKey("solar_schedule.id"))
    # solar = relationship("SolarSchedule")

    # clocked_id = sa.Column(sa.INTEGER,sa.ForeignKey("clocked_schedule.id"))
    # clocked = relationship("ClockedSchedule")
    # solar = models.ForeignKey(
    #     SolarSchedule, on_delete=models.CASCADE, null=True, blank=True,
    #     verbose_name=_('Solar Schedule'),
    #     help_text=_('Solar Schedule to run the task on.  '
    #                 'Set only one schedule type, leave the others null.'),
    # )
    # clocked = models.ForeignKey(
    #     ClockedSchedule, on_delete=models.CASCADE, null=True, blank=True,
    #     verbose_name=_('Clocked Schedule'),
    #     help_text=_('Clocked Schedule to run the task on.  '
    #                 'Set only one schedule type, leave the others null.'),
    # )

    ## 任务的位置参数
    args = sa.Column(
        sa.JSON,
        default='[]',
        comment='JSON encoded positional arguments (Example: ["arg1", "arg2"])'
    )

    ## 任务关键词参数
    kwargs = sa.Column(
        sa.JSON,
        default='{}',
        comment='JSON encoded keyword arguments (Example: {"argument": "value"})',
    )

    ## 任务被beat发送到的broker的队列
    queue = sa.Column(
        sa.String(256),
        nullable=True,
        default=None,
        comment='该定时任务对应的发送的Queue,Queue必须是已经在celery的CELERY_TASK_QUEUES中定义',
    )

    # you can use low-level AMQP routing options here,
    # but you almost certaily want to leave these as None
    # http://docs.celeryproject.org/en/latest/userguide/routing.html#exchanges-queues-and-routing-keys
    exchange = sa.Column(
        sa.String(256),
        nullable=True,
        default=None,
        comment='Override Exchange for low-level AMQP routing',
    )
    routing_key = sa.Column(
        sa.String(256),
        nullable=True,
        default=None,
        comment='Override Routing Key for low-level AMQP routing',
    )
    headers = sa.Column(
        sa.JSON,
        default='{}',
        comment='JSON encoded message headers for the AMQP message.',
    )

    ## 任务的优先级
    priority = sa.Column(
        sa.INTEGER,
        default=None,
        nullable=True,
        comment='Priority Number between 0 and 255. Supported by: RabbitMQ, Redis (priority reversed, 0 is highest).'
    )
    
    ## 过期日期
    expires = sa.Column(
        sa.DateTime,
        nullable=True,
        comment='Datetime after which the schedule will no longer trigger the task to run',
    )
    ## 过期时间:秒
    expire_seconds = sa.Column(
        sa.Integer,
        nullable=True,
        comment='Timedelta with seconds which the schedule will no longer trigger the task to run',
    )
    ## 是否只运行一次
    one_off =  sa.Column(
        sa.BOOLEAN,
        default=False,
        comment='If True, the schedule will only run the task a single time',
    )

    ## 开始运行时间
    start_time = sa.Column(
        sa.DateTime,
        nullable=True,
        comment='Datetime when the schedule should begin triggering the task to run',
    )
    ## 是否启用该定时任务
    enabled =  sa.Column(
        sa.BOOLEAN,
        default=True,
        comment='Set to False to disable the schedule',
    )

    ## 最后一次运行时间
    last_run_at = sa.Column(
        sa.DateTime, 
        nullable=True,
        comment='Datetime that the schedule last triggered the task to run. Reset to None if enabled is set to False.',
    )
    ## 已经运行总数
    total_run_count = sa.Column(
        sa.INTEGER,
        default=0, 
        comment='Running count of how many times the schedule has triggered the task',
    )
    ## 最后一次修改时间
    date_changed = sa.Column(
        sa.DateTime,
        comment='Datetime that this PeriodicTask was last modified',
    )

    ## 任务描述
    description = sa.Column(
        sa.TEXT,
        comment='Detailed description about the details of this Periodic Task',
    )

    # objects = managers.PeriodicTaskManager() WHAT THIS?
    # 如果是scheduler运行过程中堆model的修改，no_changes字段会被设置为true,
    # 不会触发修改到 periodic-tasks表
    no_changes = False

    def _clean_expires(self):
        if self.expire_seconds is not None and self.expires:
            raise RuntimeError(
                'Only one can be set, in expires and expire_seconds'
            )

    @property
    def expires(self):
        return self.expires or self.expire_seconds

    def __str__(self):
        fmt = '{0.name}: {{no schedule}}'
        if self.interval:
            fmt = '{0.name}: {0.interval}'
        # if self.crontab:
        #     fmt = '{0.name}: {0.crontab}'
        # if self.solar:
        #     fmt = '{0.name}: {0.solar}'
        # if self.clocked:
        #     fmt = '{0.name}: {0.clocked}'
        return fmt.format(self)

    @property
    def schedule(self):
        """该定时任务对应的schedule"""
        if self.interval:
            return self.interval.schedule
        if self.crontab:
            return self.crontab.schedule
        # if self.solar:
        #     return self.solar.schedule
        # if self.clocked:
        #     return self.clocked.schedule



@event.listens_for(PeriodicTask, 'after_delete')
def receive_after_delete(mapper, connection, target):
    # print("delete Periodic Task")
    update_change_time(connection,target)

@event.listens_for(PeriodicTask, 'after_insert')
def receive_after_insert(mapper, connection, target):
    # print("insert Periodic Task")
    update_change_time(connection,target)

@event.listens_for(PeriodicTask, 'after_update')
def receive_after_update(mapper, connection, target):
    print("update Periodic Task")
    update_change_time(connection,target)


from sqlalchemy import select,insert,update
def update_change_time(conn,instance):
    ### 这里是更新修改记录表
    # if not instance.no_changes:
    if not instance.no_changes:
        logger.info(f"detect a periodic change, update table[periodic_tasks] change time...:{datetime.datetime.now()}")
        record = conn.execute(select(PeriodicTasks).where(PeriodicTasks.id == 1)).fetchone()
        if record:
            conn.execute(update(PeriodicTasks).where(PeriodicTasks.id == 1).values(last_update=datetime.datetime.now()))
        else:
            conn.execute(insert(PeriodicTasks).values(last_update=datetime.datetime.now()))


if __name__=="__main__":
    import sqlalchemy.orm.identity
    # import threading
    # import sqlalchemy.orm.state
    # from sqlalchemy import inspect
    # DeclarativeBase.metadata.create_all(engine)
    # session1 = SessionFactory()

    
    # tasks=PeriodicTask(
    #     interval_id=1,                  # we created this above.
    #     name='TestTask1-23',          # simply describes this periodic task.
    #     task='core.celery.test_task',  # name of task.
    #     args=8,
    # )
    # session1.add(tasks)
    # session1.commit()


    # t = threading.Thread(target=tar)
    # t.start()
    # t.join()
    # res = session.query(PeriodicTask).get(1)
    # res.name="Ssss"
    # session.delete(res)
    # # session.flush()
    # print(inspect(res).deleted,inspect(res).detached,inspect(res).pending)
    
    # print(session.identity_map._dict,session.new,session.deleted,session.dirty)
    # res = session.query(PeriodicTask).get(6)
    # print(session.identity_map._dict