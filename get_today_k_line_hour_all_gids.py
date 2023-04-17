import requests as req
import re
from collections import deque

def get_timeline_page(code, page):
    url = 'https://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradedetail.php?symbol=' + code
    if (page > 1):
        url = url + '&page=' + str(page)
    content = req.get(url).text
    startIndex = content.find('datatbl')
    content = content[startIndex:len(content) - 1]
    endIndex = content.find('</table>')
    content = content[0:endIndex - 1]
    startIndex = content.find('<tbody>')
    content = content[startIndex:len(content) - 1]
    pattern = re.compile(r'<tr(.*?)</tr>')
    patternTh = re.compile(r'<th(.*?)</th')
    patternTd = re.compile(r'<td(.*?)</td')
    result = pattern.findall(content)
    i = len(result) - 1
    timeLine = deque([])
    while(i >= 0):
        resultTh = patternTh.findall(result[i])
        resultTd = patternTd.findall(result[i])
        time = ''
        price = ''
        volume = ''
        amount = ''
        if (len(resultTh) > 0):
            time = resultTh[0]
            time = time[1:len(time)-1]
        if (len(resultTd) > 4):
            price = resultTd[0]
            price = price[1:len(price) - 1]
            volume = resultTd[3]
            volume = volume[1:len(volume) - 1]
            amount = resultTd[4]
            amount = amount[1:len(amount) - 1]
        if (time != '' and price != '' and volume != '' and amount != ''):
            timeLineObj = [time, price, volume, amount]
            timeLine.append(timeLineObj)
        i = i - 1
    return timeLine


def get_timeline(code):
    i = 1
    totalTimeLine = []
    finish = False

    while (not(finish)):
        haveDone = False
        currentTimeLine = get_timeline_page(code, i)
        k = len(currentTimeLine) - 1
        while (k >= 0):
            j = 0
            exist = False
            while (j < len(totalTimeLine)):
                if (totalTimeLine[j][0] == currentTimeLine[k][0]
                        and totalTimeLine[j][1] == currentTimeLine[k][1]
                        and totalTimeLine[j][2] == currentTimeLine[k][2]
                        and totalTimeLine[j][3] == currentTimeLine[k][3]):
                    exist = True
                    break
                j = j + 1
            if (not(exist)):
                totalTimeLine.append(currentTimeLine[k])
                haveDone = True
            k = k - 1
        if (not(haveDone)):
          finish = True
        else:
            i = i + 1
    return totalTimeLine

util = __import__('util')
redis = util.redis_client
all_gids = redis.smembers('all_gids')
while (len(all_gids) > 0):
    code = all_gids.pop()
    str_code = str(code)
    str_code = str_code.split(' ')[0].strip().replace('b\'', '')
    print(str(len(all_gids)) + ' ' + str_code)





get_timeline('sh600031')