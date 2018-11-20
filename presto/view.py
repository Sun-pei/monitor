import time
import threading
from model import presdb,infdb
from util import getlogger
from sqls import recvsql,sendsql
from functools import lru_cache

logger = getlogger()
#"[(None, '发送-留言通知'), (0.8294270833333334, '发送-订单取消通知'),\
#            (0.8563829787234043, '发送-订单支付成功通知')]"
@lru_cache(128)  #取数据
def dates(sql):
    res = presdb.fetch_all(sql)
    return res

def w2influx(date,meas):
    json_body = []
    for x in date:
        line = {
            "measurement": "{}".format(meas),
            "tags": {
                "type": "{}".format(x[1])
            },
            # "time": "{}".format(time.time()),
            "fields": {
                "value": float(x[0]) if x[0] else float(-1)
            }
        }
        json_body.append(line)
    infdb.write(json_body)  # 写入
    # print(json_body)

def pres2influx():
    flag = False
    try:
        recv = dates(recvsql)
        send = dates(sendsql)
        w2influx(recv, "recv")
        w2influx(send,"send")
        flag = True
    except Exception as e:
        print(e)
        logger.error(e)
    finally:
        presdb.close()


class Pysettimer(threading.Thread):
    '''
    Pysettimer is simulate the C++ settimer ,
    it need  pass funciton pionter into the class ,
    timeout and is_loop could be default , or customized
    '''
    def __init__(self, function, args=None,timeout=5,is_loop=True):
        threading.Thread.__init__(self)
        self.event=threading.Event()
        # inherent the funciton and args
        self.function=function
        self.args=args      # pass a tuple into the class
        self.timeout=timeout
        self.is_loop=is_loop

    def run(self):
        while not self.event.is_set():
            self.event.wait(self.timeout) # wait until the time eclipse

            # self.function(self.args)
            self.function()
            if not  self.is_loop:
                self.event.set()

    def stop(self):
        self.event.set()










