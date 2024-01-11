from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import  time



util = __import__('util')

def get_big_deal_per_gid(gid, browser):
    browser.get('https://vip.stock.finance.sina.com.cn/quotes_service/view/cn_bill.php?symbol=' + gid)
    browser.implicitly_wait(2000)
    dateDrop = Select(browser.find_element(By.ID, 'selectTradeDate'))
    get_data(gid, browser)
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
    print(dateStr)
    sum_totalvol = browser.find_element(By.ID, 'sum-totalvol').text
    sum_stockvol = browser.find_element(By.ID, 'sum-stockvol').text
    sum_totalamt = browser.find_element(By.ID, 'sum-totalamt').text
    sum_stockamt = browser.find_element(By.ID, 'sum-stockamt').text
    sum_avgprice = browser.find_element(By.ID, 'sum-avgprice').text
    sum_kuvolume = browser.find_element(By.ID, 'sum-kuvolume').text
    sum_kdvolume = browser.find_element(By.ID, 'sum-kdvolume').text
    sum_kevolume = browser.find_element(By.ID, 'sum-kevolume').text
    print(gid + ' ' + dateStr + ' ' + sum_totalvol + ' ' + sum_stockvol + ' ' + sum_totalamt + ' ' + sum_stockamt
          + ' ' + sum_avgprice + ' ' + sum_kuvolume + ' ' + sum_kdvolume + ' ' + sum_kevolume)

chrome = webdriver.Chrome()
redis = util.redis_client

all_gids = redis.smembers('all_gids')
while (len(all_gids) > 0):
    code = all_gids.pop()
    str_code = str(code)
    str_code = str_code.split(' ')[0].strip().replace('b\'', '')
    if (str_code.startswith('sh60') or str_code.startswith('sz00') or str_code.startswith('sz30')):
        get_big_deal_per_gid(str_code, chrome)
    print(str(len(all_gids)) + ' ' + str_code)
    #time.sleep(1)
    #break





#chrome.get('https://vip.stock.finance.sina.com.cn/quotes_service/view/cn_bill.php?symbol=sh605081')