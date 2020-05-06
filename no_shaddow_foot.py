util = __import__('util')


def scan_foot(key_str):
    gid = key_str.split('_')[0]
    foot = {'gid': gid}
    r = util.redis_client
    timeline_list = r.zrange(key_str, 0, -1)
    if (timeline_list.__len__() < 10):
        return {'gid': gid}
    i = 0
    current_settle = 0
    last_settle = 0
    current_low = 0
    last_low = 0
    for timeline in timeline_list:
        timeline_str_arr = timeline.decode('utf-8').split(',')
        if (i > 0):
            last_settle = current_settle
            last_low = current_low
        current_settle = float(timeline_str_arr[2])
        current_low = float(timeline_str_arr[4])
        if (current_low < last_low and (last_settle - current_low)/current_low >= 0.01
            and (current_settle - current_low)/current_low >= 0.01):
            if (foot.__len__() == 1):
                foot = {'gid': gid, 'foot_time': timeline_str_arr[0], 'price': current_low, 'rate': (current_settle - current_low)/current_low}
            else:
                if (foot['price']>current_low):
                    foot = {'gid': gid, 'foot_time': timeline_str_arr[0], 'price': current_low, 'rate': (current_settle - current_low)/current_low}
                else:
                    foot = {'gid': gid}
        else:
            if (foot.__len__() > 1):
                if (foot['price']>current_low):
                    foot = {'gid': gid}
        i = i + 1
    return foot

#print(scan_foot('002469_20200430_quotes'))

def scan_foot_for_day(date_str):
    conn = util.get_sql_server_conn()
    gid_list = util.redis_client.keys('*' + date_str + '*')
    for gid in gid_list:
        foot = scan_foot(gid.decode('utf-8'))
        cursor = conn.cursor()
        if (foot.__len__() > 1):
            cursor2 = conn.cursor()
            sql = "select 'a' from alert_foot_new where gid = '" + util.get_8_digit_gid(foot['gid']) + "' and alert_date = '" \
                + date_str[0:4] + '-' + date_str[4:6] + '-' + date_str[6:9] + "' and foot_time = '" + str(foot['foot_time']) + "' "
            exists = False
            with (cursor2.execute(sql)):
                if (cursor2.fetchone()):
                    exists = True
            cursor2.close()
            if (not(exists)):
                sql = " update alert_foot_new set valid = 0 where gid = '" + util.get_8_digit_gid(foot['gid']) \
                      + "' and alert_date = '" + date_str[0:4] + '-' + date_str[4:6] + '-' + date_str[6:9] + "' "
                cursor.execute(sql)
                sql = "insert into alert_foot_new (gid, alert_date, foot_time, foot_price, foot_rate) values ('" \
                    + util.get_8_digit_gid(foot['gid']) + "', '" + date_str[0:4] + '-' + date_str[4:6] + '-' + date_str[6:9] \
                    + "', '" + str(foot['foot_time']) + "', " + str(foot['price']) + ", " + str(foot['rate']) + " )"
                cursor.execute(sql)
        else:
            sql = " update alert_foot_new set valid = 0 where gid = '" + util.get_8_digit_gid(foot['gid']) \
                  + "' and alert_date = '" + date_str[0:4] + '-' + date_str[4:6] + '-' + date_str[6:9] + "' "
            cursor.execute(sql)
        cursor.commit()
        cursor.close()
    conn.close()



scan_foot_for_day('20200506')