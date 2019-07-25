# Yajun Li  2019.6.27
################################################################################
# Indexes are used to retrieve data from the database very fast.
# The users cannot see the indexes, they are just used
# to speed up searches/queries.

# or CREATE UNIQUE INDEX, then duplicate values are not allowed
execution_1 = """CREATE INDEX idx_lastname
ON Persons (LastName);"""

# index on multiple columns
execution_2 = """CREATE INDEX idx_pname
ON Persons (LastName, FirstName);"""

# not sure below
execution_3 = """DROP INDEX index_name ON table_name;"""

# Project: INDEX the local database


import psycopg2
try:
     con = psycopg2.connect("dbname='postgres' host = '192.168.31.44' port='5432' user='postgres'  password='123456'")
except:
    print("I am unable to connect to the database")
cur = con.cursor()

# 1
# Print PostgreSQL version
cur.execute("SELECT version();")
record = cur.fetchone()
print("You are connected to - ", record, "\n")

if con:
    cur.close()
    con.close()
    print("PostgreSQL connection is closed")

# 2
# query all database names
try:
    conn = psycopg2.connect("dbname='postgres' host = '192.168.31.44' port='5432' user='postgres'  password='123456'")
except:
    print("I am unable to connect to the database")
cur = conn.cursor()
cur.execute("""SELECT datname from pg_database""")

# define an object to store the result
rows = cur.fetchall()

if con:
    cur.close()
    con.close()
    print("PostgreSQL connection is closed")


# 3
# list table names in a database
try:
     con = psycopg2.connect("dbname='postgres' host = '192.168.31.44' port='5432' user='postgres'  password='123456'")
except:
    print("I am unable to connect to the database")
cur = con.cursor()
cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
for table in cur.fetchall():
    print(table)
# from database postgres, I get
# ('stock_tick',)
# ('orderbookfactor',)
# ('ticktodailyfactors',)
# ('yajunli',)
# ('company',)
# ('stocktick',)

if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")


# 4
# fetch the columns  from table 'stocktick'

try:
     con = psycopg2.connect("dbname='postgres' host = '192.168.31.44' port='5432' user='postgres'  password='123456'")
except:
    print("I am unable to connect to the database")

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

if con:
    cur.close()
    con.close()
    print("PostgreSQL connection is closed")


# 5
# make the index by columns 'c_stock_code', 'c_date_time'
try:
     con = psycopg2.connect("dbname='postgres' host = '192.168.31.44' port='5432' user='postgres'  password='123456'")
except:
    print("I am unable to connect to the database")

execution_2 = """CREATE INDEX idx_pname
ON stocktick (c_stock_code, c_date_time)"""

cur = con.cursor()
cur.execute(execution_2)

if con:
    cur.close()
    con.close()
    print("PostgreSQL connection is closed")













