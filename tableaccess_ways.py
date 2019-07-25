import sqlsoup
# 1
from sqlalchemy import inspect

def create_engine():
    from sqlalchemy import create_engine
    return create_engine("postgres+psycopg2://postgres:123456@192.168.31.44/postgres", echo=True)


def test_sqlsoup():
    engine = create_engine()
    db = sqlsoup.SQLSoup(engine)
    # Note: database must have a table called 'company' for this example
    users = db.company.all()
    print(users)
    # return users


if __name__ == "__main__":
     test_sqlsoup()


# 2
from sqlalchemy import create_engine
engine = create_engine("postgres+psycopg2://postgres:123456@192.168.31.44:5432/postgres", echo=True)

inspector = inspect(engine)

    for table_name in inspector.get_table_names():
       for column in inspector.get_columns(table_name):
           print("Column: %s" % column['name'])

# 3
from sqlalchemy import MetaData
m = MetaData()
m.reflect(engine)
for table in m.tables.values():
    print(table.name)
    for column in table.c:
        print(column.name)









