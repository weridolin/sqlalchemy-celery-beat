
NEVER_CHECK_TIMEOUT = 100000000
import pytz,os,datetime,time
from functools import partial

TZ = pytz.timezone(os.environ.get("TIME_ZONE",'Asia/Shanghai'))

def add_tz(value):
    """
        本地时间添加时区信息：
        2023-01-17 13:39:01.324367 tz='Asia/Shanghai' -> 2023-01-17 13:39:01.324367+08:00
    """ 
    if isinstance(value,str):
        value = datetime.datetime.strptime(value,"%Y-%m-%d %H:%M:%S")
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
    if isinstance(native_time,str):
        native_time = datetime.datetime.strptime(native_time,"%Y-%m-%d %H:%M:%S")  
    local_dt = TZ.localize(native_time, is_dst=None)
    return local_dt.astimezone(pytz.utc)

def to_native(utc_time):
    """
        utc时间根据时区转换为本地时间
    """
    now_stamp = time.time()
    local_time = datetime.datetime.fromtimestamp(now_stamp)
    utc_time = datetime.datetime.utcfromtimestamp(now_stamp)
    offset = local_time - utc_time
    local = utc_time+offset
    local_dt = TZ.localize(local, is_dst=None)
    return  local_dt.astimezone(TZ)

def now():
    if TZ:
        return add_tz(datetime.datetime.now())
    else:
        return datetime.datetime.now()

if __name__ =="__main__":
    # print(add_tz(datetime.datetime.now()))
    print(now())
    # print(to_utc(datetime.datetime.now()))
    print(to_native(datetime.datetime.utcnow()))
    # local_dt = TZ.localize(datetime.datetime.now(), is_dst=None)
    ...