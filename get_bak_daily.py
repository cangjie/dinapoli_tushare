import sys
import datetime
from datetime import datetime as date
import tushare as ts
util = __import__('util')
redis = util.redis_client
pipe = util.redis_pipe

token = '4da2fbec9c2cee373d3aace9f9e200a315a2812dc11267c425010cec'
ts.set_token(token)
pro = ts.pro_api()





def get_money_flow(nowDateStr):
    timestamp = util.get_timestamp(nowDateStr, '%Y%m%d')
    df = pro.bak_basic(trade_date=nowDateStr, fields='ts_code,pe,float_share,holder_num')
    dfMoney = pro.moneyflow(trade_date=nowDateStr)
    i = 0
    while (i < df.index.size):
        oriCode = df['ts_code'][i]
        head = oriCode[7:9].lower()
        if (oriCode[0:3] == '688'):
            head = 'kc'
        gid = head + oriCode[0:6]
        key = gid + '_kline_day'
        values = redis.zrangebyscore(key, timestamp, timestamp)
        if (len(values) > 0):
            j = 0
            done = False
            while (j < len(values)):
                newValue = str(values[j])
                valueArr = newValue.split(',')

                if (len(valueArr) == 9 and not done):

                    buy_sm_vol = 0
                    sell_sm_vol = 0
                    buy_md_vol = 0
                    sell_md_vol = 0
                    buy_lg_vol = 0
                    sell_lg_vol = 0
                    buy_elg_vol = 0
                    sell_elg_vol = 0
                    net_mf_vol = 0
                    k = 0
                    while(k < dfMoney.index.size):
                        if (dfMoney['ts_code'][k] == oriCode):
                            buy_sm_vol = dfMoney['buy_sm_vol'][k]
                            sell_sm_vol = dfMoney['sell_sm_vol'][k]
                            buy_md_vol = dfMoney['buy_md_vol'][k]
                            sell_md_vol = dfMoney['sell_md_vol'][k]
                            buy_lg_vol = dfMoney['buy_lg_vol'][k]
                            sell_lg_vol = dfMoney['sell_lg_vol'][k]
                            buy_elg_vol = dfMoney['buy_elg_vol'][k]
                            sell_elg_vol = dfMoney['sell_elg_vol'][k]
                            net_mf_vol = dfMoney['net_mf_vol'][k]
                            break
                        k = k + 1

                    newValue = newValue.replace("b", "").replace("'", "") + ',' + str(df['pe'][i])
                    newValue = newValue + ',' + str(df['float_share'][i]) + ',' + str(df['holder_num'][i])
                    newValue = newValue + ',' + str(buy_sm_vol) + ',' + str(sell_sm_vol)
                    newValue = newValue + ',' + str(buy_md_vol) + ',' + str(sell_md_vol)
                    newValue = newValue + ',' + str(buy_lg_vol) + ',' + str(sell_lg_vol)
                    newValue = newValue + ',' + str(buy_elg_vol) + ',' + str(sell_elg_vol)
                    newValue = newValue + ',' + str(net_mf_vol)
                    print(newValue)
                    pipe.zrem(key, values[j])
                    pipe.zadd(key, {newValue: timestamp})
                    pipe.persist(key)
                    done = True
                j = j + 1
        i = i + 1
    pipe.execute()
    try:
        redis.bgsave()
    except Exception as err:
        print(str(err))

nowDate = date.now()
startDate = nowDate
endDate = nowDate

if (len(sys.argv) == 2):
    startDate = date.strptime(sys.argv[1], '%Y%m%d')
    endDate = startDate
if (len(sys.argv) == 3):
    startDate = date.strptime(sys.argv[1], '%Y%m%d')
    endDate = date.strptime(sys.argv[2], '%Y%m%d')

currentDate = startDate
while (currentDate <= endDate):
    currentDateStr = currentDate.strftime('%Y%m%d')
    try:
        get_money_flow(currentDateStr)
    except Exception as err:
        print(currentDateStr + ' ' + str(err))
    finally:
        currentDate = currentDate + datetime.timedelta(days=1)





#nowDateStr = nowDate.strftime('%Y%m%d')
#startDateStr = nowDateStr
#endDateStr = nowDateStr
#if (len(sys.argv) > 1):
#    nowDateStr = sys.argv[1]