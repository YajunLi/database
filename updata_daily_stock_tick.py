# -*-coding : utf-8 -*-
import pandas as pd
import numpy as np
import os
import logging
from gmsdk.api import StrategyBase
from sqlalchemy import create_engine
import warnings

warnings.filterwarnings('ignore')

columns_name = ['c_market', 'c_stock_code', 'c_date_time', 'c_price', 'c_current',
                'c_money', 'c_vol', 'c_flag', 'c_buy1_price', 'c_buy2_price',
                'c_buy3_price', 'c_buy4_price', 'c_buy5_price', 'c_sell1_price',
                'c_sell2_price', 'c_sell3_price', 'c_sell4_price', 'c_sell5_price',
                'c_buy1_quantity', 'c_buy2_quantity', 'c_buy3_quantity',
                'c_buy4_quantity', 'c_buy5_quantity', 'c_sell1_quantity',
                'c_sell2_quantity', 'c_sell3_quantity', 'c_sell4_quantity',
                'c_sell5_quantity']


class Logger(object):
    def __init__(self):
        log_file = os.path.join(os.path.dirname(os.getcwd()), r"log_file//{}_log.txt".format(pd.datetime.now().date()))
        self.log_file_path = log_file
        file_handler = logging.FileHandler(self.log_file_path, 'a', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(filename)s[line: %(lineno)d] :  %(message)s"))

        self.logger = logging.Logger('TickDataToDatabase', level=logging.INFO)
        self.logger.addHandler(file_handler)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)


def getLogger():
    logger = Logger()
    return logger


def update_stock_tick(date=None):
    """
    将csv数据读取到数据库中
    date: 日期
    :return: no return
    """

    conn = create_engine('postgres+psycopg2://postgres:123456@localhost:5432/postgres', echo=True)

    ret = StrategyBase('13554406825', 'qq532357515')
    sh_stocks = ret.get_instruments(exchange='SHSE', sec_type=1, is_active=1)
    sz_stocks = ret.get_instruments(exchange='SZSE', sec_type=1, is_active=1)
    stocks = sh_stocks + sz_stocks
    stocks = [i.symbol for i in stocks]

    for data in get_stock_tick_data(stocks, date):
        insert_data(data, conn)


def insert_data(data, con):
    """
    插入单个csv文件入库
    :param data: DataFrame数据
    :param con: 数据库连接
    :return: no return
    """

    # string_data_io = io.StringIO()
    # pd_sql_engine = pd.io.sql.pandasSQL_builder(con)
    # data.to_csv(string_data_io, sep='|', index=False)
    try:
        # table = pd.io.sql.SQLTable('stocktick', pd_sql_engine, frame=data,
        #                            index=False, if_exists='append', schema='public')
        # table.create()
        # string_data_io.seek(0)
        # string_data_io.readline()  # remove header
        # with con.connect() as connection:
        #     with connection.connection.cursor() as cursor:
        #         copy_cmd = "COPY public.stocktick FROM STDIN HEADER DELIMITER '|' CSV"
        #         cursor.copy_expert(copy_cmd, string_data_io)
        #     connection.connection.commit()
        logger = getLogger()
        code = data['c_stock_code'].iloc[0]
        date = data['c_date_time'].iloc[0].date()
        pd.io.sql.to_sql(data, 'stocktick', con=con, schema='public', if_exists='append', index=False)
        logger.info('finish inserting {}--{} tick data'.format(code, date))

    except Exception as info:
        logger.info(info)


def get_stock_tick_data(stock_codes, date):
    """
    获取股票当天的tick数据
    :param stock_codes:  股票代码
    :param date: 日期
    :return:
    """
    ret = StrategyBase('13554406825', 'qq532357515')
    date = pd.to_datetime(date).strftime('%Y-%m-%d')
    for code in stock_codes:
        st = date + ' 09:30:00'
        et = date + ' 15:00:00'
        data = ret.get_ticks(code, st, et)
        data = [props(d) for d in data]
        data = pd.DataFrame(data)
        data = clean_data(data)
        yield data


def clean_data(data):
    """
    清洗数据
    :param data: DataFrame
    :return: 清洗后的数据
    """
    global columns_name
    if len(data) == 0:
        return data
    else:
        data = data[data.last_amount != 0]  # 剔除成交量为0的股票
    # 对数据进行清洗以及格式变换

    data.rename(columns={'sec_id': 'c_stock_code', 'strtime': 'c_date_time', 'last_price': 'c_price',
                         'last_volume': 'c_vol', 'last_amount': 'c_money', 'trade_type': 'c_flag'}, inplace=True)

    data['c_market'] = data['exchange'].map(lambda x: x[:2].lower())
    data['c_date_time'] = data['c_date_time'].map(lambda x: pd.to_datetime(x[:19]))
    data['c_flag'] = data['c_flag'].map(lambda x: 'S' if x == 8 else 'B')
    data['c_current'] = data['c_vol']
    try:
        data = data[columns_name]
    except Exception as e:
        print(e)

    data.drop_duplicates(subset=['c_stock_code', 'c_date_time'], keep='last', inplace=True)
    return data


def props(obj):
    """
    将class转化为字典
    :param obj:
    :return:
    """
    pr = {}
    for name in dir(obj):
        value = getattr(obj, name)
        if not name.startswith('__') and not callable(value):
            pr[name] = value
        if name in ['asks', 'bids']:
            pr = get_book_order_info(name, pr)
    return pr


def get_book_order_info(name, pr):
    key = 'c_buy' if name == 'bids' else 'c_sell'
    for i in range(5):
        try:
            pr[key + '{}_price'.format(i + 1)], pr[key + '{}_quantity'.format(i + 1)] = pr[name][i]
            pr[key + '{}_price'.format(i + 1)] = float('%.2f' % pr[key + '{}_price'.format(i + 1)])
            pr[key + '{}_quantity'.format(i + 1)] = np.int8(pr[key + '{}_quantity'.format(i + 1)])
        except:
            pr[key + '{}_price'.format(i + 1)], pr[key + '{}_quantity'.format(i + 1)] = 0, 0
    pr.pop(name)
    return pr


if __name__ == '__main__':
    for i in pd.bdate_range("2019-06-11", "2019-06-11"):
        date = i.strftime("%Y-%m-%d")
        update_stock_tick(date=date)
