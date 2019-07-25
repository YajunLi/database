import psycopg2


# 1
try:
    conn = psycopg2.connect("dbname='postgres' host = '127.0.0.1' port='5432' user='postgres'  password='12345678'")
    conn = psycopg2.connect("dbname='postgres' host = '192.168.31.94' port='5432' user='julin'  password='123456'")
    conn = psycopg2.connect("dbname='postgres' host = '192.168.31.44' port='5432' user='postgres'  password='123456'")

except:
    print("I am unable to connect to the database")

# We can create as many cursors as we want from a single connection
# object. Cursors created from the same connection are not isolated,
# i.e., any changes done to the database by a cursor are immediately
# visible by the other cursors.
# Cursors are not thread-safe.
cur = conn.cursor()

# Print PostgreSQL version
cur.execute("SELECT version();")
record = cur.fetchone()
print("You are connected to - ", record, "\n")

if(conn):
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")


# 2
# query all database names
try:
    conn = psycopg2.connect("dbname='postgres' host = '127.0.0.1' port='5432' user='postgres'  password='12345678'")
except:
    print("I am unable to connect to the database")
cur = conn.cursor()
cur.execute("""SELECT datname from pg_database""")

# define an object to store the result
rows = cur.fetchall()

# see the result
rows
type(rows)
# or
for row in rows:  # notice that row is a tuple
    print("   ", row[0])   # print(row) is the same
cur.close()
# commit the changes, in this case, there is no change
conn.commit()

if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")

# 3
# drop database below
try:
    conn = psycopg2.connect("dbname='yajunli' host = '127.0.0.1' port='5432' user='postgres'  password='12345678'")
except:
    print("I am unable to connect to the database")

try:
    conn.set_isolation_level(0)  # this line is added
    cur = conn.cursor()
    cur.execute("""DROP DATABASE try""")
except:
    print("I can't drop the database!")
if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")


# 4
# fetch the content of the database or table???
try:
    conn = psycopg2.connect("dbname='yajunli' host = '127.0.0.1' port='5432' user='postgres'  password='12345678'")
except:
    print("I am unable to connect to the database")
cur = conn.cursor()
try:
    cur.execute("""SELECT * from public.company""")
except:
    print("I can't SELECT from company")
rows = cur.fetchall()
rows
if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")


# 5
# create a table named company  try text[] for company_name
# also bigint for company_id
# create vendors
try:
    conn = psycopg2.connect("dbname='postgres' host = '127.0.0.1' port='5432' user='postgres'  password='12345678'")
except:
    print("I am unable to connect to the database")
cur = conn.cursor()
cur.execute("""
        CREATE TABLE vendors (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
        """)
cur.close()
# commit the changes
conn.commit()
conn.close()


# 6
# not sure how to delete a table. Below is not successful
try:
    conn = psycopg2.connect("dbname='postgres' host = '127.0.0.1' port='5432' user='postgres'  password='12345678'")
except:
    print("I am unable to connect to the database")
cur = conn.cursor()
cur.execute("DROP TABLE yajunli ")
cur.close()
# commit the changes
conn.commit()
conn.close()


# queries below success in the query window in pgadmin4
# insert into company(company_name) values ('Hangseng Bank')
# CREATE TABLE yajunli ( id INTEGER NOT NULL,
#                              PRIMARY KEY(id) )

# 7
# select items from table
try:
    conn = psycopg2.connect("dbname='yajunli' host = '127.0.0.1' port='5432' user='postgres'  password='12345678'")
except:
    print("I am unable to connect to the database")
cur = conn.cursor()
try:
    cur.execute("""SELECT * from company""")  # or public.yajunli
except:
    print("I can't SELECT from company")
rows = cur.fetchall()
rows
# commit the changes
conn.commit()
if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")


# 8
# write something into the table
try:
    conn = psycopg2.connect("dbname='postgres' host = '127.0.0.1' port='5432' user='postgres'  password='12345678'")
except:
    print("I am unable to connect to the database")
cur = conn.cursor()
try:
    cur.execute("""insert into vendors(vendor_name) values ('Jimmy')""")
except:
    print("I can't write into vendors")
# commit the changes
conn.commit()
if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")


# 9
# conditional select
try:
    conn = psycopg2.connect("dbname='postgres' host = '127.0.0.1' port='5432' user='postgres'  password='12345678'")
except:
    print("I am unable to connect to the database")
cur = conn.cursor()
try:
    cur.execute("""SELECT * FROM vendors WHERE vendor_id = '2'""")
except:
    print("I can't select from vendors")
# commit the changes
conn.commit()
if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")


# 10
# delete data in a table
# first, create a table named part in the database postgres
try:
    conn = psycopg2.connect("dbname='postgres' host = '127.0.0.1' port='5432' user='postgres'  password='12345678'")
except:
    print("I am unable to connect to the database")
cur = conn.cursor()
try:
    cur.execute("""CREATE TABLE parts (
                   part_id SERIAL PRIMARY KEY,
                   part_name VARCHAR(255) NOT NULL
    )""")
except:
    print("I can't select from vendors")
# commit the changes
conn.commit()
if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")

# second, insert some data points into the table parts, it does not work
try:
    conn = psycopg2.connect("dbname='postgres' host = '127.0.0.1' port='5432' user='postgres'  password='12345678'")
except:
    print("I am unable to connect to the database")
cur = conn.cursor()
sql = """INSERT INTO parts(part_name)
          VALUES('%s') RETURNING part_id;"""
variable = 'SIM Tray' 'Speaker'  'Vibrator'  'Antenna'  'Home Button'  'LTE Modem'
try:
    cur.execute(sql, variable)
except:
    print("I can't insert into parts")
# commit the changes
conn.commit()
if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")
# now, delete row 2 using the function delete_part()
if __name__ == '__main__':
    deleted_rows = delete_part(2)
    print('The number of deleted rows: ', deleted_rows)





# 9
# insert into multiple rows using dictionary, namedict is a tuple of three dictionaries
namedict = ({"first_name": "Joshua", "last_name": "Drake"},
            {"first_name": "Steven", "last_name": "Foo"},
            {"first_name": "David", "last_name": "Bar"})
cur = conn.cursor()
cur.executemany("""INSERT INTO yajunli(firstname,lastname) VALUES (%(first_name)s, %(last_name)s)""", namedict)
# commit the changes
conn.commit()
if conn:
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")



























