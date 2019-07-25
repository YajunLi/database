import pandas as pd
from sqlalchemy import create_engine
import io
import os
import logging

logger = logging.getLogger('TickDataToDatabase')
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("..\log_file\\log_%s.txt" %pd.datetime.now().date())
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)



def insert_csv_to_database(files):
    """
    将csv数据读取到数据库中
    :param files:  csv数据列表
    :return: no return
    """
    conn = create_engine('postgres+psycopg2://julin:123456@localhost:5432/BrooksCapital', echo=True)
    pd_sql_engine = pd.io.sql.pandasSQL_builder(conn)



    try:
        file = files
        logger.info('get csv file :%s' % file)
    except IndexError :
        print("queue empty, please hold on")

    # 读取csv文件，并导入至数据库
    data = pd.read_csv(file, encoding='ANSI', index_col=0, )
    data = data.rename(columns={'factor':'factors'})
    data = data[['date', 'stock_code', 'open','high', 'low', 'close', 'prev_close', 'is_paused','is_st',
                 'turnover', 'volume', 'factors']]
    string_data_io = io.StringIO()
    data.to_csv(string_data_io, sep='|', index=False)
    try:
        # pd.io.sql.to_sql(data,'stocktick',con=conn,schema='public',if_exists='append', index=False)
        table = pd.io.sql.SQLTable('daily_stock_price_info', pd_sql_engine, frame=data,
                                   index=False, if_exists='append', schema='public')
        table.create()
        string_data_io.seek(0)
        string_data_io.readline()  # remove header
        with conn.connect() as connection:
            with connection.connection.cursor() as cursor:
                copy_cmd = "COPY public.daily_stock_price_info FROM STDIN HEADER DELIMITER '|' CSV"
                cursor.copy_expert(copy_cmd, string_data_io)
            connection.connection.commit()

    except Exception as info:
        logger.info(info)

    logger.info('%s :data write over ' % file)



if __name__ == '__main__':
    files = 'stocks_daily_data.csv'
    insert_csv_to_database(files)
