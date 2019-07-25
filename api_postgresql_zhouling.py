import pandas as pd
from sqlalchemy import create_engine
import io
import os
import logging

logger = logging.getLogger('TickDataToDatabase')
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("log_file\\log_%s.txt" %pd.datetime.now().date())
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

user_name = "julin"
password = "123456"
pg_name = "BrooksCapital"


class DataCenter(object):

    def __init__(self, database, user_name, password):
        self.con = create_engine('postgres+psycopg2://{}:{}@localhost:5432/{}'.
                                    format(user_name,password,database), echo=True)

    def get_ticks(self, codes, start_date, end_date):
        """
        获取tick数据
        :param codes: 股票代码, 格式为str或者list, str表示单个股票代码, list为一组股票代码
        :param start_date: 起始日期, datetime格式或者str格式
        :param end_date: 结束日期, datetime格式或者str格式
        :return:
        """
        codes = tuple(codes) if isinstance(codes, list) else tuple([codes]*2)
        sql = """select * from stocktick as a  where a.c_stock_code in {} and 
                                                     a.c_date_time >= '{}' and 
                                                     a.c_date_time <= '{}' """.format(codes,
                                                                                   start_date,
                                                                                   end_date)
        data = pd.read_sql(sql=sql, con=self.con)
        return data


if __name__ == '__main__':
    user_name = "julin"
    password = "123456"
    pg_name = "BrooksCapital"
    data_center = DataCenter(pg_name, user_name, password)

    codes = "000002"
    start_date = "2018-01-01"
    end_date = "2018-01-03"
    data = data_center.get_ticks(codes, start_date, end_date)
    pass
