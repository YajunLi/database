from ezalchemy import EZAlchemy

DB = EZAlchemy(
    db_user='postgres',
    db_password='123456',
    db_hostname='192.168.31.44',
    db_database='postgres',
    d_n_d='postgres'   # stands for dialect+driver
)

# this function loads all tables in the database to the class instance DB
DB.connect()

# List all associations to DB, you will see all the tables in that database
dir(DB)