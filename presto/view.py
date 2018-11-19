from model import presdb,infdb
from util import getConfig,getlogger
from sqls import recvsql,sendsql
import datetime

#"[(None, '发送-留言通知'), (0.8294270833333334, '发送-订单取消通知'),\
#            (0.8563829787234043, '发送-订单支付成功通知')]"
def pres2influx():
    #取数据
    recv = presdb.fetch_all(recvsql)
    send = presdb.fetch_all(sendsql)
    #拼json
    json_body = []
    for x in recv:
        line={
            "measurement":"recv",
            "tags": {
                "type": "{}".format(x[1])
            },
            "time": "{}".format(datetime.datetime.now()),
            "fields": {
                "value":"{}".format(x[0])
            }
        }
        # print(line)
        json_body.append(line)
    infdb.write(json_body) #写入

    json_body1 = []
    for y in send:
        line1={
            "measurement":"send",
            "tags": {
                "type": "{}".format(y[1])
            },
            "time": "{}".format(datetime.datetime.now()),
            "fields": {
                "value":"{}".format(y[0])
            }
        }
        # print(line)
        json_body1.append(line1)
    infdb.write(json_body1)  #写入










