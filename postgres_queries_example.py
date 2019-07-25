execution_1 = """SELECT *
  FROM sales
 ORDER BY sale_date DESC
 FETCH FIRST 10 ROWS ONLY"""

# A pipelined top-N query doesn't need to read and sort the entire result set.
# If there is no suitable index on SALE_DATE for a pipelined order by,
# the database must read and sort the entire table.
# TABLE ACCESS BY INDEX ROWID is highly efficient

execution_2 = """SELECT column1, column2, ...
FROM table_name
ORDER BY column1, column2, ... ASC|DESC;"""

execution_3 = """SELECT * FROM Customers
ORDER BY Country ASC, CustomerName DESC;"""

execution_4 = """SELECT * FROM Customers
WHERE Country='Mexico';"""

# numerical fields should not be enclosed in quotes
execution_5 = """SELECT * FROM Customers
WHERE CustomerID=1;"""

# <>, >=, !=, between, like, in
execution_6 = """SELECT * FROM Products
WHERE Price BETWEEN 50 AND 60;"""

execution_7 = """SELECT * FROM Customers
WHERE City IN ('Paris','London');"""

execution_8 = """SELECT * FROM Customers
WHERE City LIKE 's%';"""


# if all of the columns are inserted into
execution_9 = """INSERT INTO table_name
VALUES (value1, value2, value3, ...);"""

execution_10 = """INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country)
VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');"""

# wildcard, % selects any number of characters
# _ represents a single character
# - represents a range of characters
execution_11 = """SELECT * FROM Customers
WHERE City LIKE 'ber%';"""

# selects any city containing the pattern es
execution_12 = """SELECT * FROM Customers
WHERE City LIKE '%es%';"""

# The values of BETWEEN can be numbers, text, or dates.
execution_13 = """SELECT * FROM Products
WHERE Price BETWEEN 10 AND 20
AND NOT CategoryID IN (1,2,3);"""

execution_14 = """SELECT * FROM Products
WHERE ProductName BETWEEN 'Carnarvon Tigers' AND 'Mozzarella di Giovanni'
ORDER BY ProductName;"""

# choose between dates
execution_15 = """SELECT * FROM Orders
WHERE OrderDate BETWEEN #01/07/1996# AND #31/07/1996#;"""

execution_16 = """SELECT * FROM Orders
WHERE OrderDate BETWEEN '1996-07-01' AND '1996-07-31';"""

# set limit
execution_17 = """SELECT column_name(s)
FROM table_name
WHERE condition
LIMIT number;"""

# select top
execution_18 = """SELECT TOP 50 PERCENT * FROM Customers;"""

