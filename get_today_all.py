import tushare
import time
i = 0;
while i < 100:
    print(str(i)+'\r\n')
    df = tushare.get_today_all()
    time.sleep(30)
    i = i + 1