import tushare as ts
import pyodbc
import time
util = __import__('util')

def update_turnover(gid, date_time, turnover):
    conn = util.get_sql_server_conn()
    #date_str = str(datetime.now().strftime('%Y-%m-%d'))
    gid = util.get_8_digit_gid(gid)
    cursor = conn.cursor()
    exists = False
    with cursor.execute("select * from turnover where alert_date = '" + date_time + "' and gid = '" + gid + "' "):
        if (cursor.fetchone()):
            exists = True
    conn.close()
    conn = util.get_sql_server_conn()
    cursor = conn.cursor()
    if (exists):
        tsql = "update turnover set turnover_rate = " + str(turnover) + " where gid = '" + gid + "' and alert_date = '" + date_time + "' "
    else:
        tsql = "insert into turnover values ('" + gid + "', '" + date_time + "', " + str(turnover) + ")"
    cursor.execute(tsql)
    cursor.commit()
    conn.close()

date_str = time.strftime('%Y-%m-%d')
df = ts.get_today_all()
i = 0
while (i < df['turnoverratio'].size):
    print(date_str + ' ' + df['code'][i] + ' ' + str(df['turnoverratio'][i])+'\r\n')
    update_turnover(df['code'][i], date_str, float(df['turnoverratio'][i]))
    i = i + 1
