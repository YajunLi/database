# Yajun Li  2019.6.27
########################################################################################
#
execution_1 = """CREATE VIEW [Brazil Customers] AS
SELECT CustomerName, ContactName
FROM Customers
WHERE Country = "Brazil";"""

execution_2 = """SELECT * FROM [Brazil Customers];"""


execution_3 = """CREATE VIEW [Products Above Average Price] AS
SELECT ProductName, Price
FROM Products
WHERE Price > (SELECT AVG(Price) FROM Products);"""

execution_4 = """SELECT * FROM [Products Above Average Price];"""







