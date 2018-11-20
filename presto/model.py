from pyhive import presto
import datetime
from presto.util import getlogger
from util import getConfig
from influxdb import InfluxDBClient

PDB = "prestodb"
PHOST = getConfig(PDB,"host")
PPORT = getConfig(PDB,"port")
# print(HOST,PORT)

class Presto:

    def __init__(self, host=None,port=None):
        self._logger = getlogger()
        if host is None:
            self._host = PHOST
        else:
            self._host = host
        if port is None:
            self._port = PPORT
        else:
            self._port = port

        self._conn = self.connectPresto()

        if (self._conn):
            self._cursor = self._conn.cursor()

    # 数据库连接
    def connectPresto(self):
        conn = False
        try:
            conn = presto.connect(host=self._host,port=self._port)
            cursor = conn.cursor()

        except Exception as data:
            self._logger.error("connect presto failed, %s" % data)
            conn = False
        return conn

    # 获取查询结果集
    def fetch_all(self,sql:str,d=datetime.date.today()):
        res = ''
        if (self._conn):
            try:
                fsql =sql.format(d,d)
                self._cursor.execute(fsql)
                res = self._cursor.fetchall()
            except Exception as data:
                res = False
                self._logger.warn("query presto exception, %s" % data)
        return res

    def update(self, sql):
        flag = False
        if (self._conn):
            try:
                self._cursor.execute(sql)
                self._conn.commit()
                flag = True
            except Exception as data:
                flag = False
                self._logger.warn("update presto exception, %s" % data)

        return flag

    # 关闭数据库连接
    def close(self):
        if (self._conn):
            try:
                if (type(self._cursor) == 'object'):
                    self._cursor.close()
                if (type(self._conn) == 'object'):
                    self._conn.close()
            except Exception as data:
                self._logger.warn("close presto exception, %s,%s,%s" % (data, type(self._cursor), type(self._conn)))


IDB = "influxdb"
IHOST = getConfig(IDB,"HOST")
IPORT = getConfig(IDB,"port")
WDB = getConfig(IDB,"database")


class Influx:

    def __init__(self, host=None,port=None,database=None):
        self._logger = getlogger()
        if host is None:
            self._host = IHOST
        else:
            self._host = host
        if port is None:
            self._port = IPORT
        else:
            self._port = port
        if database is None:
            self._database = WDB
        else:
            self._database = database

        self._client = self.connectInflux()

    # 数据库连接
    def connectInflux(self):
        client = False
        try:
            client = InfluxDBClient(host=self._host,port=self._port,database=self._database)
            if {'name': self._database} not in client.get_list_database():
                client.create_database(self._database)
        except Exception as data:
            self._logger.error("connect influx failed, %s" % data)
            client = False
        return client

    # 获取查询结果集
    def select(self, sql:str):
        res = ''
        if (self._client):
            try:
                res = self._client.query(sql)
            except Exception as data:
                res = False
                self._logger.warn("query influx exception, %s" % data)
        return res

    def create_db(self,name):
        flag = False
        if (self._client):
            try:
                # if self.sele
                self._client.create_database(name)
                flag = True
            except Exception as data:
                self._logger.error("write influx failed, %s" % data)
                flag = False
            return flag

    def write(self,json_body):
        flag = False
        if (self._client):
            try:
                self._client.write_points(json_body)
                flag = True
            except Exception as data:
                self._logger.error("write influx failed, %s" % data)
                flag = False
            return flag

    def drop(self,name):
        flag = False
        if (self._client):
            try:
                self._client.drop_database(name)
                flag = True
            except Exception as data:
                self._logger.error("drop influx failed, %s" % data)
                flag = False
            return flag





presdb = Presto()
infdb = Influx()