
import time
import datetime
import os
import pytz
import conf

NEVER_CHECK_TIMEOUT = 100000000


if conf.USE_TZ:
    TZ = pytz.timezone(conf.TIME_ZONE or "Asia/Shanghai")
else:
    TZ = None


def add_tz(value):
    """
        本地时间添加时区信息：
        2023-01-17 13:39:01.324367 tz='Asia/Shanghai' -> 2023-01-17 13:39:01.324367+08:00
    """
    if isinstance(value, str):
        value = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    if value.utcoffset():
        return value
    else:
        local = TZ.localize(value)
        value = local.astimezone(tz=TZ)
        return value


def to_utc(native_time):
    """
        本地时间根据所在时区转成对应的utc时间
    """
    if not conf.USE_TZ:
        return native_time
    if isinstance(native_time, str):
        native_time = datetime.datetime.strptime(
            native_time, "%Y-%m-%d %H:%M:%S")
    local_dt = TZ.localize(native_time, is_dst=None)
    return local_dt.astimezone(pytz.utc)


def to_native(utc_time):
    """
        utc时间根据时区转换为本地时间
    """
    if not conf.USE_TZ:
        # 不使用utc，utc_time为本地时间
        return utc_time
    now_stamp = time.time()
    local_time = datetime.datetime.fromtimestamp(now_stamp)
    utc_time = datetime.datetime.utcfromtimestamp(now_stamp)
    offset = local_time - utc_time
    local = utc_time+offset
    local_dt = TZ.localize(local, is_dst=None)
    return local_dt.astimezone(TZ)


def now():
    if conf.USE_TZ and TZ:
        return to_utc(datetime.datetime.now())
    else:
        return datetime.datetime.now()


if __name__ == "__main__":
    # print(add_tz(datetime.datetime.now()))
    print(now())
    # print(to_utc(datetime.datetime.now()))
    print(to_native(datetime.datetime.utcnow()))
    # local_dt = TZ.localize(datetime.datetime.now(), is_dst=None)
