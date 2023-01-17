from threading import Event
from celery import beat
import time
import datetime
from events import on_db_update

# 重新写 beat.Service.start,主要是为了随时能够中断两次tick之间的sleep
# todo 当处于两次tick之间的sleep状态时，在其他进程修改了数据库中的task，如何让scheduler也能监听到?:
# 1.修改scheduler的sync_every,缩小两次tick之间的最大间隔事件
# 2.others


def Service__start(self, embedded_process=False):
    beat.info('beat: Starting...(sqlalchemy-celery-beat custom start)')
    beat.debug('beat: Ticking with max interval->%s',
               beat.humanize_seconds(self.scheduler.max_interval))

    beat.signals.beat_init.send(sender=self)
    if embedded_process:
        beat.signals.beat_embedded_init.send(sender=self)
        beat.platforms.set_process_title('celery beat')

    try:
        while not self._is_shutdown.is_set():
            interval = self.scheduler.tick()
            if interval and interval > 0.0:
                beat.debug('beat: Waking up %s.',
                           beat.humanize_seconds(interval, prefix='in '))
                # time.sleep(interval)
                TimeSleepInterruption(
                    interrupt_signal=on_db_update, check_interval=1).sleep(interval=interval)
                if self.scheduler.should_sync():
                    self.scheduler._do_sync()
    except (KeyboardInterrupt, SystemExit):
        self._is_shutdown.set()
    finally:
        self.sync()


beat.Service.start = Service__start


class TimeSleepInterruption:
    """
        time.sleep 过程中能够随便中断.简单来说就是把interval拆成无数个小块去sleep,
        这里不用循环,直接判断截止的时间,因为循环的话,如果循环的间隔过小,误差会很大
    """

    def __init__(self, interrupt_signal, check_interval=0.001) -> None:
        """
            interrupt_signal:终止sleep的信号
        """
        assert isinstance(
            interrupt_signal, Event), "interrupt_signal can only be instance of Event"
        self.interrupt_signal = interrupt_signal
        self.event = Event()
        self.check_interval = check_interval  # 检验sleep是否到点的间隔时长

    def sleep(self, interval: float):
        self.interrupt_signal.clear()
        deadline = datetime.datetime.now()+datetime.timedelta(seconds=interval)
        while datetime.datetime.now() <= deadline:
            self.event.wait(self.check_interval)
            if self.interrupt_signal.is_set():
                break
