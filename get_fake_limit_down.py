import tushare as ts
util = __import__('util')
redis = util.redis_client
all_gids = redis.smembers('all_gids')


token = '4da2fbec9c2cee373d3aace9f9e200a315a2812dc11267c425010cec'
ts.set_token(token)

conn = util.get_sql_server_conn()
#cursor = conn.cursor()


def save_limit_down(date, gid):
    date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    sql = "insert into limit_down (gid, alert_date, fake ) values ('" + gid + "', '" + date + "' , 1)"
    try:
        conn.execute(sql)
        #conn.commit()
    except Exception as err:
        print(str(err))
    finally:
        conn.commit()
    #print(gid)

def deal_realtime_quote(gids):
    df = ts.realtime_quote(gids)
    print(df)
    i = 0
    while (i < df['TS_CODE'].size):
        code = str(df['TS_CODE'][i])
        gid = code[7:9].lower()+code[0:6]
        settle = float(df['PRE_CLOSE'][i])
        current = float(df['PRICE'][i])
        date = df['DATE'][i]
        if (settle == 0):
            return
        pct = (current - settle) * 100 / settle
        if (gid[0:3]=='sz3'):
            if (pct < -19 and pct > - 20):
                save_limit_down(date, gid)
        else:
            if (pct < -9 and pct > -10):
                save_limit_down(date, gid)
        i = i + 1



gidList = ''
while (len(all_gids)>0):
    gid = str(all_gids.pop().decode(encoding='utf-8')).split(' ')[0]
    if (gid[0:2]=='kc'):
        continue
    code = gid[2:8] + '.' + gid[0:2].upper()

    if (gidList == ''):
        gidList = code
    else:
        gidList = gidList + ',' + code
    if (len(gidList.split(',')) == 50):
        #print(gidList + '\r\n')
        deal_realtime_quote(gidList)
        gidList = ''
#print(gidList)
deal_realtime_quote(gidList)
conn.close()
