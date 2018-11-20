from presto.view import pres2influx,Pysettimer
import time

if __name__ == '__main__':
    mytime = Pysettimer(pres2influx)

    mytime.start()
    time.sleep(30)
    mytime.stop()
    print("time over")


