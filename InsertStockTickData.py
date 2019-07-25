# -*-coding : utf-8 -*-
import pandas as pd
import os
import io
import re
from threading import Thread
import logging
from functools import reduce
from sqlalchemy import create_engine
import sqlalchemy

logger = logging.getLogger('TickDataToDatabase')
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("..\log_file\\log.txt")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)


columns_name = ['c_market', 'c_stock_code', 'c_date_time', 'c_price', 'c_current',
                'c_money', 'c_vol', 'c_flag', 'c_buy1_price', 'c_buy2_price',
                'c_buy3_price', 'c_buy4_price', 'c_buy5_price', 'c_sell1_price',
                'c_sell2_price', 'c_sell3_price', 'c_sell4_price', 'c_sell5_price',
                'c_buy1_quantity', 'c_buy2_quantity', 'c_buy3_quantity',
                'c_buy4_quantity', 'c_buy5_quantity', 'c_sell1_quantity',
                'c_sell2_quantity', 'c_sell3_quantity', 'c_sell4_quantity',
                'c_sell5_quantity']


def get_csvfile():
    dir = r'E:\stocktick\tick2011y'
    files = reduce(lambda x, y: x + y,
                   list(map(lambda x: list(map(lambda z: os.path.join(x[0], z), x[2])), os.walk(dir))))
    target_file_codes = pd.read_excel('stock_pool.xlsx', header=None, encoding='ANSI')
    target_file_codes.dropna(axis=1, inplace=True)
    stock_code_pattern = re.compile(r'\d{6}')
    target_file_codes = list(map(lambda x: stock_code_pattern.findall(x)[0], target_file_codes[1].tolist()))
    csv_files = list(filter(lambda x: x.endswith('.csv') and stock_code_pattern.findall(x)[-1] in target_file_codes,
                            files))  # 给定代码的csv文件
    return csv_files


def insert_csv_to_database(file_queue):
    """
    将csv数据读取到数据库中
    :param file_queue:  csv数据列表
    :return: no return
    """
    global  columns_name, logger
    conn = create_engine('postgres+psycopg2://julin:123456@localhost:5432/BrooksCapital', echo=True)
    pd_sql_engine = pd.io.sql.pandasSQL_builder(conn)

    while 1:
        try:
            file = file_queue.pop()
            logger.info('get csv file :%s' % file)
        except IndexError :
            print("queue empty, please hold on")
            break
        # 读取csv文件，并导入至数据库
        data = pd.read_csv(file, encoding='ANSI', dtype=str)
        data.columns = columns_name
        data.drop_duplicates(subset=['c_stock_code', 'c_date_time'], keep='last', inplace=True)
        string_data_io = io.StringIO()
        data.to_csv(string_data_io, sep='|', index=False)
        try:
            # pd.io.sql.to_sql(data,'stocktick',con=conn,schema='public',if_exists='append', index=False)
            table = pd.io.sql.SQLTable('stocktick', pd_sql_engine, frame=data,
                                       index=False, if_exists='append', schema='public')
            table.create()
            string_data_io.seek(0)
            string_data_io.readline()  # remove header
            with conn.connect() as connection:
                with connection.connection.cursor() as cursor:
                    copy_cmd = "COPY public.stocktick FROM STDIN HEADER DELIMITER '|' CSV"
                    cursor.copy_expert(copy_cmd, string_data_io)
                connection.connection.commit()

        except Exception as info:
            logger.info(info)
            del data
            continue
        del data
        logger.info('%s :data write over ' % file)


def main():
    # process_list = []
    # for i in range(3):
    #     files = list(filter(lambda  x: csv_files.index(x)%3==i, csv_files))
    #     p = Thread(target=insert_csv_to_database, args=(files,))
    #     process_list.append(p)
    #     p.daemon = True
    #     p.start()
    # for p in process_list:
    #     p.join()
    csv_files = get_csvfile()
    insert_csv_to_database(csv_files)
    logger.info('finish data inserting!!!')


if __name__ == '__main__':
    main()
