#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gmsdk.api import StrategyBase
from gmsdk.util import bar_to_dict
import pandas as pd
import logging
logger = logging.getLogger('insert_cat_tick_data')
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("log_file\\%s_CTA" %pd.datetime.now().date())
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

class MyStrategy(StrategyBase):
    def __init__(self, *args, **kwargs):
        super(MyStrategy, self).__init__(*args, **kwargs)
        self.oc = True


def handle_all_cta_daily_data(date):

    types = pd.read_excel(r'cta_type.xlsx')
    ticks_list = []
    for n in range(types.shape[1]):
        code = types.iloc[1,n]
        data = handle_daily_tick(code, date)
        logger.info(r"%s -- %s --%s 条数据" % (code, date, len(data)))
        ticks_list.append(data)
    return pd.concat(ticks_list)


def handle_daily_tick(code, date):
    """
    处理单个品种一天的数据, 包括所有活跃的品种
    :param code: 期货代码, str
    :param date: 日前, timestamp
    :return:  DataFrame
    """
    ret = MyStrategy(
        username='13554406825',
        password='qq532357515',
        strategy_id='strategy_2'
    )

    # 设置夜盘与白天交易时间
    daily_start_time = date.strftime('%Y-%m-%d 09:00:00')
    daily_end_time = date.strftime('%Y-%m-%d 15:00:00')

    """ 夜盘因为规则不一样， 设置为字典形式， key值为交易所代码， value对应为时间段，
        其中CFFEX为中金所的代码，CZCE为郑州商品交易所代码，SHFE为上海商品交易所代码，DCE为大连商品交易所代码"""
    night_start_time = {'SHFE':date.strftime('%Y-%m-%d 21:00:00'),
                        "DCE":date.strftime('%Y-%m-%d 21:00:00'),
                        "CZCE":date.strftime('%Y-%m-%d 21:00:00')
                        }
    night_end_time = {'SHFE': (date+pd.Timedelta('1d')).strftime('%Y-%m-%d 02:30:00'),
                        "DCE": date.strftime('%Y-%m-%d 23:30:00'),
                        "CZCE": date.strftime('%Y-%m-%d 23:30:00')
                        }

    # -------------------------- 计算当天需要录入数据库的品种代码 ----------------------------
    # up_month = list(map(lambda x: x > str(date.month), months))
    # current_month = list(map(lambda x: x == str(date.month), months))
    # low_month = list(map(lambda x: x < str(date.month), months))
    # month_code = []
    # up_month_codes = list(map(lambda x:code + date.replace(month=int(x)).strftime('%y%m'), up_month))
    # current_month_codes = list(map(lambda x:code + date.strftime('%y%m'), current_month))
    # current_month_codes_1 = list(map(lambda x: code + date.replace(year=date.year+1).strftime('%y%m'), current_month))
    # current_month_codes.extend(current_month_codes_1)
    # low_month_codes = list(map(lambda x:code + date.replace(month=int(x), year=date.year+1).strftime('%y%m'), low_month))
    #
    # # -------------------------- 整理需要入库的代码到到一个列表 ------------------------------
    # month_code.extend(up_month_codes)
    # month_code.extend(current_month_codes)
    # month_code.extend(low_month_codes)

    #  ---------- 整理单个品种近月到连4的代码 ---------------
    month_code_0 = ['00', '01', '02', '03', '04']
    month_code = list(map(lambda i: code+i, month_code_0))


    # ------------------------------- 取tick数据 ---------------------------------------------
    data_list = []
    for contract in month_code:
        market_code = code.split('.')[0]
        daily_data = ret.get_ticks(contract, daily_start_time, daily_end_time)
        if len(daily_data)>30000:
            logger.info(r"%s -- %s --%s 条数据" % (contract, date, 30000))
        try:
            night_data = ret.get_ticks(contract, night_start_time[market_code], night_end_time[market_code])
            daily_data.extend(night_data)
            if len(night_data) > 30000:
                logger.info(r"%s -- %s --%s 条数据" % (contract, date, 30000))
        except:
            logger.info(r'%s %s has error' %(contract, date))
            pass

        daily_data = list(map(lambda x: transfrom_obj_to_dataframe(x), daily_data))
        data_list.extend(daily_data)
    return pd.DataFrame(data_list)


def transfrom_obj_to_dataframe(data):
    """
    将obj数据转换为dataframe格式
    :param data: 掘金 tick object
    :return: dataframe
    """
    pr = {}
    for name in dir(data):
        value = getattr(data, name)
        if not name.startswith('__') and not callable(value):
            pr[name] = value

    if len(pr['asks']) == 1:
        pr['asks_price'] = pr['asks'][0][0]
        pr['asks_volume'] = pr['asks'][0][1]
    else:
        pr['asks_price'] = 0
        pr['asks_volume'] = 0

    if len(pr['bids']) == 1:
        pr['bids_price'] = pr['bids'][0][0]
        pr['bids_volume'] = pr['bids'][0][1]
    else:
        pr['bids_price'] = 0
        pr['bids_volume'] = 0
    del pr['asks'], pr['bids']

    return pr


def main():

    start = '2018-10-01'
    end = '2018-12-01'
    data_list = []
    for date in pd.bdate_range(start=start, end=end):
        daily_tick_data = handle_all_cta_daily_data(date)
        data_list.append(daily_tick_data)

    data = pd.concat(data_list)
    data.to_csv('tickdata_01.csv')


if __name__ == '__main__':
    main()

