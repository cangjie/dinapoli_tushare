import datetime
import pyodbc
util = __import__('util')
config = __import__('config')
all_gids_arr = util.redis_client.smembers('all_gids')
redis = util.redis_client

gid_arr = []
for gid_name_pair in all_gids_arr:
    gid_name_pair_str = gid_name_pair.decode(encoding='utf-8')
    gid_arr.append(gid_name_pair_str.split(' ')[0])

def update_avarage(date_str, gid, current_price, avarage_price, percent):
    sql = " delete alert_avarage_timeline where alert_date = '" + date_str[0:4] + "-" + date_str[4:6] + "-" \
          + date_str[6:9] + "' and gid = '" + gid + "' "
    conn = util.get_sql_server_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    if (percent > 0.7 and current_price > avarage_price):
        sql = " insert into  alert_avarage_timeline (alert_date, gid, current_price, avarage_price) values ('" \
              + date_str[0:4] + "-" + date_str[4:6] + "-" + date_str[6:9] + "', '" + gid + "', " + str(current_price) \
              + ", " + str(avarage_price) + ") "
        try:
            cursor.execute(sql)
        except Exception as err:
            print('err')
    try:
        cursor.commit()
    except Exception as exp:
        print('err2')
    finally:
        cursor.close()
        conn.close()


def compute_avarage(formated_today):
    #formated_today = today.strftime('%Y%m%d')
    redis = util.redis_client
    for gid in gid_arr:
        key_str = gid[2:8]+'_'+formated_today+'_quotes'
        timeline_list = redis.zrange(key_str, 0, -1)
        if (timeline_list.__len__() < 2):
            continue
        avarage_price = 0
        last_volume = 0
        i = 0
        over_avarage_times = 0
        for timeline in timeline_list:
            timeline_arr = timeline.decode('utf-8').split(',')
            if (i==0):
                avarage_price = float(timeline_arr[2])
                last_volume = float(timeline_arr[5])
            else:
                total_amount = avarage_price * float(last_volume)
                current_volume = float(timeline_arr[5])
                if (current_volume==0):
                    i = i + 1
                    continue;
                current_price = float(timeline_arr[2])
                total_amount = total_amount + current_price * (current_volume - last_volume)
                avarage_price = total_amount / current_volume
                last_volume = current_volume
                if (current_price > avarage_price):
                    over_avarage_times = over_avarage_times + 1
            i = i + 1
        print(formated_today + ' ' + gid + ' ' + str(round(avarage_price, 2)) + ' ' +str(round(100*over_avarage_times/float(timeline_list.__len__()), 2))+'%')
        update_avarage(formated_today, gid, current_price, avarage_price, over_avarage_times/float(timeline_list.__len__()))



#compute_avarage(datetime.date.today().strftime('%Y%m%d'))

compute_avarage('20200513')
compute_avarage('20200514')
compute_avarage('20200515')

compute_avarage('20200518')
compute_avarage('20200519')
compute_avarage('20200520')
compute_avarage('20200521')
compute_avarage('20200522')



compute_avarage('20200525')
compute_avarage('20200526')
compute_avarage('20200527')
compute_avarage('20200528')
compute_avarage('20200529')



compute_avarage('20200601')
compute_avarage('20200602')
compute_avarage('20200603')
compute_avarage('20200604')
compute_avarage('20200605')
