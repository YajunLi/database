# Yajun Li  2019.6.27
##################################################################################
import psycopg2
conn = psycopg2.connect("dbname='postgres' host = '192.168.31.94' port='5432' user='julin'  password='123456'")
cur = conn.cursor()

# 1
cur.execute("""SELECT datname from pg_database""")
rows = cur.fetchall()
rows
# 'postgres', 'StockTickTest', 'BrooksCapital'
if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")


# 2  see the first ten rows from the table named stocktick in the database BrooksCapital
con = psycopg2.connect("dbname='BrooksCapital' host = '192.168.31.94' port='5432' user='julin'  password='123456'")
cur = con.cursor()
cur.execute("""SELECT * FROM stocktick 
             LIMIT 10""")
rows = cur.fetchall()rows


if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")


# 3 two functions:
def table_exists(con, table_str):

    exists = False
    try:
        cur = con.cursor()
        cur.execute("select exists(select relname from pg_class where relname='" + table_str + "')")
        exists = cur.fetchone()[0]
        print(exists)
        cur.close()
    except psycopg2.Error as e:
        print(e)
    return exists


def get_table_col_names(con, table_str):

    col_names = []
    try:
        cur = con.cursor()
        cur.execute("select * from " + table_str + " LIMIT 0")
        for desc in cur.description:
            col_names.append(desc[0])
        cur.close()
    except psycopg2.Error as e:
        print(e)

    return col_names

# another way to list the col names
execute_col = """SELECT column_name FROM information_schema.columns WHERE table_name = 'stocktick' ORDER BY column_name """

# 4
# great job! below is to see the table names in a database, i.e. BrooksCapital
con = psycopg2.connect("dbname='BrooksCapital' host = '192.168.31.94' port='5432' user='julin'  password='123456'")
cur = con.cursor()
cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
for table in cur.fetchall():
    print(table)
# from BrooksCapital, I get
# 'orderbookfactor','daily_stock_price_info','tmp_tickdailyfactor',
# 'stocktick','ticktodailyfactors','1min_bar_data',
if con:
    cur.close()
    con.close()
    print("PostgreSQL connection is closed")


# 5  grab tick data, well done!
con = psycopg2.connect("dbname='BrooksCapital' host = '192.168.31.94' port='5432' user='julin'  password='123456'")
cur = con.cursor()
cur.execute("""SELECT * FROM stocktick
       WHERE c_stock_code = '300124'
       AND c_date_time BETWEEN '2018-02-01' AND '2019-06-01'""")
stock_300124 = cur.fetchall()


if con:
    cur.close()
    con.close()
    print("PostgreSQL connection is closed")

# save the data into csv locally
import pandas as pd
Frame = pd.DataFrame(stock_300124, columns=col_names)
Frame.to_csv('300124.csv', index=False)







