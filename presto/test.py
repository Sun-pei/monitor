# from pyhive import presto
# import datetime
# from sqls import recvsql
#
# conn = presto.connect(host="presto.corp.pangqiu.com",port=8888)
# cursor = conn .cursor()
# d = datetime.date.today()
# cursor.execute(recvsql.format(d,d))
# print(cursor.fetchall())
#
# from influxdb import InfluxDBClient
#
# json_body = [
#     {
#         "measurement": "cpu_load_short",
#         "tags": {
#             "host": "server01",
#             "region": "us-west"
#         },
#         "time": "2009-11-10T23:00:00Z",
#         "fields": {
#             "value": 0.64
#         }
#     }
# ]
#
# client = InfluxDBClient('localhost', 8086, 'root', 'root', 'example')
#
# client.create_database('example')
#
# client.write_points(json_body)
#
# result = client.query('select value from cpu_load_short;')
#
# print("Result: {0}".format(result))
from model import infdb

print(infdb.select("select * from send limit 10"))




