from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import  time
import requests
class Fund:
    gid = ''

def deal_table(browser):
    div = browser.find_element(By.ID, 'history_table')
    trArr = div.find_elements(By.TAG_NAME, 'tr')
    i = 0
    fundArr = []
    while (i < len(trArr)):
        try:
            if (i < 2):
                i = i + 1
                continue
            fund = Fund()
            tr = trArr[i]
            tdArr = tr.find_elements(By.TAG_NAME, 'td')
            j = 0
            while(j < len(tdArr)):
                td = tdArr[j]
                txt = td.get_attribute('innerText')
                if (j==0):
                    fund.alert_date = txt[0:4] + '-' + txt[4:6] + '-' + txt[6:8]
                if (j==1):
                    fund.settle = txt
                if (j==2):
                    txt = txt.replace('%', '')
                    fund.rate = str(float(txt)/100)
                if (j==3):
                    fund.flow_amount = txt
                if (j==4):
                    fund.flow_amount_5_avarage = txt
                if (j==5):
                    fund.big_flow_amount = txt
                if (j==6):
                    txt = txt.replace('%', '')
                    fund.big_percent = str(float(txt)/100)
                if (j==7):
                    fund.mid_flow_amount = txt
                if (j==8):
                    txt = txt.replace('%', '')
                    fund.mid_percent = str(float(txt)/100)
                if (j==9):
                    fund.small_flow_amount = txt
                if (j==10):
                    txt = txt.replace('%', '')
                    fund.small_percent = str(float(txt)/100)
                j = j + 1
        except Exception as err  :
            print(err)
        fundArr.append(fund)
        i = i + 1
    return fundArr

def save_fund(fund):
    url = 'http://weixin.goldenma.xyz/api/BigDeal/UpdateFund/' + fund.gid + '?alertDate=' + str(fund.alert_date) \
    + '&settle=' + str(fund.settle) + '&rate=' + str(fund.rate) + '&flow_amount=' + str(fund.flow_amount) \
    + '&flow_amount_5_avarage=' + str(fund.flow_amount_5_avarage) + '&big_flow_amount=' + str(fund.big_flow_amount) \
    + '&big_percent=' + str(fund.big_percent) + '&mid_flow_amount=' + str(fund.mid_flow_amount) + '&mid_percent=' + str(fund.mid_percent) \
    + '&small_flow_amount=' + str(fund.small_flow_amount) + '&small_percent=' + str(fund.small_percent)
    try:
        res = requests.get(url)
        if (res.status_code != 200):
            print(url)
    except:
        print('err')

    return

util = __import__('util')
redis = util.redis_client
all_gids = redis.smembers('all_gids')
browser = webdriver.Chrome()
browser.minimize_window()
k = 0
while (len(all_gids) > 0):
    k = k + 1
    if (k == 1000):
        browser.close()
        browser.quit()
        browser = webdriver.Chrome()
        k = 0
    code = all_gids.pop()
    str_code = str(code)
    ori_code = str_code.split(' ')[0].strip().replace('b\'', '')

    if (not(ori_code.startswith('sh60')) and   not(ori_code.startswith('sz00') ) and not(ori_code.startswith('sz30'))):
        continue

    str_code = str_code.split(' ')[0].strip().replace('b\'', '').replace('sh', '').replace('sz', '').replace('kc', '')
    print(str(len(all_gids)) + ' ' + str_code)
    url = 'https://stockpage.10jqka.com.cn/' + str_code + '/funds'
    browser.get(url)
    browser.implicitly_wait(5000)
    #time.sleep(1)
    try:

        fundArr = deal_table(browser)
        i = 0
        while (i < len(fundArr)):
            fundArr[i].gid = ori_code
            save_fund(fundArr[i])
            i = i + 1
    except:
        continue

