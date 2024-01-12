import urllib.parse

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import  time
import requests



util = __import__('util')
k = 0
def get_big_deal_per_gid(gid, browser):
    #browser.minimize_window()
    browser.get('https://vip.stock.finance.sina.com.cn/quotes_service/view/cn_bill.php?symbol=' + gid)
    browser.implicitly_wait(2000)
    get_data(gid, browser)
    dateDrop = Select(browser.find_element(By.ID, 'selectTradeDate'))
    i = 1
    while (i <= 4):
        dateDrop.select_by_index(i)
        i = i + 1
        browser.implicitly_wait(2000)
        get_data(gid, browser)




def get_data(gid, browser):
    dateDrop = Select(browser.find_element(By.ID, 'selectTradeDate'))
    dateStr = ((dateDrop.first_selected_option.text.replace('年','-'))
               .replace('月','-')).replace('日', '')
    #print(dateStr)
    sum_totalvol = browser.find_element(By.ID, 'sum-totalvol').text.replace(',','')
    sum_stockvol = browser.find_element(By.ID, 'sum-stockvol').text.replace(',','')
    sum_totalamt = browser.find_element(By.ID, 'sum-totalamt').text.replace(',','')
    sum_stockamt = browser.find_element(By.ID, 'sum-stockamt').text.replace(',','')
    sum_avgprice = browser.find_element(By.ID, 'sum-avgprice').text.replace(',','')
    sum_kuvolume = browser.find_element(By.ID, 'sum-kuvolume').text.replace(',','')
    sum_kdvolume = browser.find_element(By.ID, 'sum-kdvolume').text.replace(',','')
    sum_kevolume = browser.find_element(By.ID, 'sum-kevolume').text.replace(',','')

    #sum_totalvol = 'aaa'
    if (sum_totalvol != '' and sum_stockvol != '' and sum_totalamt != '' and sum_stockamt != '' and sum_avgprice != '' \
    and sum_kuvolume != '' and sum_kdvolume != '' and sum_kevolume != ''):
        url = 'http://weixin.goldenma.xyz/api/BigDeal/UpdateBigDeal?gid=' + gid + '&alertDate=' + dateStr + '&bigDealVol=' + sum_totalvol \
        + '&totalVol=' + sum_stockvol + '&bigDealAmount=' + sum_totalamt + '&totalAmount=' + sum_stockamt + '&bigDealAvaragePrice=' + sum_avgprice \
        + '&uVol=' + sum_kuvolume + '&dVol=' + sum_kdvolume + '&eVol=' + sum_kevolume
        try:
            res = requests.get(url)
            if (res.status_code != 200):
                print(gid + ' ' + dateStr + ' ' + sum_totalvol + ' ' + sum_stockvol + ' ' + sum_totalamt + ' ' + sum_stockamt \
                + ' ' + sum_avgprice + ' ' + sum_kuvolume + ' ' + sum_kdvolume + ' ' + sum_kevolume)
        except:
            print('err')
    else:
        print(gid + ' ' + dateStr + ' ' + sum_totalvol + ' ' + sum_stockvol + ' ' + sum_totalamt + ' ' + sum_stockamt \
              + ' ' + sum_avgprice + ' ' + sum_kuvolume + ' ' + sum_kdvolume + ' ' + sum_kevolume)




redis = util.redis_client
browser = webdriver.Chrome()
browser.minimize_window()
all_gids = redis.smembers('all_gids')
while (len(all_gids) > 0):
    k = k + 1
    code = all_gids.pop()
    str_code = str(code)
    str_code = str_code.split(' ')[0].strip().replace('b\'', '')
    print(str(len(all_gids)) + ' ' + str_code)
    if (str_code.startswith('sh60') or str_code.startswith('sz00') or str_code.startswith('sz30')):
        get_big_deal_per_gid(str_code, browser)
    if (k == 100):
        browser.close()
        browser.quit()
        browser = webdriver.Chrome()
        k = 0
    #time.sleep(1)
    #break





#chrome.get('https://vip.stock.finance.sina.com.cn/quotes_service/view/cn_bill.php?symbol=sh605081')