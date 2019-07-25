from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import time
import logging

from jqdatasdk import *
auth('13554406825', 'qq532357515')

logger = logging.getLogger('DailyDataToDatabase')
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("..\log_file\\log_%s.txt" %pd.datetime.now().date())
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)


def update_dailytable_data(date=None):
    """
    每日更新daily_price_info这张表
    :return:
    """
    global logger
    conn = create_engine('postgres+psycopg2://julin:123456@localhost:5432/BrooksCapital', echo=True)
    while True:
        if not date:
            today = pd.datetime.now()
            # 下午6点之后开始更新
            if today.hour < 18:
                time.sleep(20)
                continue
            date = today.date()
        else:
            date = date
        stock_info = get_all_securities(types=[], date=date)
        stock_list = stock_info.index.tolist()
        data = get_price(stock_list, date, date, fq='none', fields=['open', 'high', 'low', 'close', 'pre_close',\
                                'volume', 'money' , 'paused', 'low_limit', 'high_limit'])
        factor = get_price(stock_list, date, date, fq='pre', fields=['factor'])
        data = data.to_frame()
        factor = factor.to_frame()
        data = pd.concat([data, factor], axis=1)
        data['is_st'] = ((data['high_limit'] - data['low_limit'])/data['low_limit'] < 0.2).astype(float)
        data.drop(labels=['low_limit', 'high_limit'], axis=1, inplace=True)
        data.reset_index(inplace=True)
        data = data.rename(columns={'pre_close':'prev_close', 'money':'turnover','paused':'is_paused',
                                    'factor':'factors','major': 'date', 'minor': 'stock_code'})
        for col in ['close', 'factors', 'high', 'is_paused', 'is_st', 'low', 'open', 'prev_close', 'turnover', 'volume']:
            data[col][data.volume == 0] = np.NaN
        data['stock_code'] = data['stock_code'].map(lambda x: x[:6] + '.SH' if x.startswith('6') else   x[:6] + '.SZ')
        # data.to_sql('daily_stock_price_info', conn, if_exists='append', index=False)
        try:
            pd.io.sql.to_sql(data, 'daily_stock_price_info', con=conn, schema='public', if_exists='append', index=False)
            break
        except Exception as e:
            logger.info(e)

if __name__ == '__main__':
    update_dailytable_data('2019-01-07')
