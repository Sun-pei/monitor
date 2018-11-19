
#--计算接收与发送成功的比例
recvsql =\
"""
SELECT CASE
           WHEN sum(send) = 0 THEN 0
           ELSE sum(rec)/cast(sum(send) AS DOUBLE)
       END AS num,
       a.mess
FROM
  (SELECT count(event_name) AS send,
          substr(event_name,4) AS mess
   FROM hive.logs.external_event_log
   WHERE date = date '{}'
     AND code = 'wechatMessage'
     AND status = 'success'
     AND event_name LIKE '发送-%'
   GROUP BY substr(event_name,4)) a
LEFT JOIN
  (SELECT count(event_name) AS rec,
          substr(event_name,4) AS mess
   FROM hive.logs.external_event_log
   WHERE date = date '{}'
     AND event_name LIKE '来自-%'
   GROUP BY substr(event_name,4)) b ON a.mess = b.mess
GROUP BY a.mess
"""

#--计算发送成功的比例
sendsql =\
"""
SELECT sum(succ)/cast(sum(ALL) AS DOUBLE) AS "成功发送比例",
       a.event_name
FROM
  (SELECT count(1) AS ALL,
          event_name
   FROM hive.logs.external_event_log
   WHERE date = date '{}'
     AND code = 'wechatMessage'
     AND event_name LIKE '发送-%'
   GROUP BY event_name) a
LEFT JOIN
  (SELECT count(1) AS succ,
          event_name
   FROM hive.logs.external_event_log
   WHERE date = date '{}'
     AND code = 'wechatMessage'
     AND status = 'success'
     AND event_name LIKE '发送-%'
   GROUP BY event_name) b ON a.event_name = b.event_name
GROUP BY a.event_name
"""