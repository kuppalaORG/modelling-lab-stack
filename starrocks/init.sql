-- starrocks/init.sql
CREATE DATABASE IF NOT EXISTS order_management_starrocks;
USE order_management_starrocks;

-- Primary Key landing tables (upsert)
CREATE TABLE IF NOT EXISTS customers_landing (
  CustomerID INT NOT NULL,
  CustomerName VARCHAR(255),
  ContactName VARCHAR(255),
  Address VARCHAR(255),
  City VARCHAR(100),
  PostalCode VARCHAR(50),
  Country VARCHAR(100),
  updated_at DATETIME
)
PRIMARY KEY(CustomerID)
DISTRIBUTED BY HASH(CustomerID) BUCKETS 4
PROPERTIES ("replication_num"="1");

CREATE TABLE IF NOT EXISTS categories_landing (
  CategoryID INT NOT NULL,
  CategoryName VARCHAR(255),
  Description VARCHAR(500),
  updated_at DATETIME
)
PRIMARY KEY(CategoryID)
DISTRIBUTED BY HASH(CategoryID) BUCKETS 4
PROPERTIES ("replication_num"="1");

CREATE TABLE IF NOT EXISTS suppliers_landing (
  SupplierID INT NOT NULL,
  SupplierName VARCHAR(255),
  ContactName VARCHAR(255),
  Address VARCHAR(255),
  City VARCHAR(100),
  PostalCode VARCHAR(50),
  Country VARCHAR(100),
  Phone VARCHAR(50),
  updated_at DATETIME
)
PRIMARY KEY(SupplierID)
DISTRIBUTED BY HASH(SupplierID) BUCKETS 4
PROPERTIES ("replication_num"="1");

CREATE TABLE IF NOT EXISTS shippers_landing (
  ShipperID INT NOT NULL,
  ShipperName VARCHAR(255),
  Phone VARCHAR(50),
  updated_at DATETIME
)
PRIMARY KEY(ShipperID)
DISTRIBUTED BY HASH(ShipperID) BUCKETS 4
PROPERTIES ("replication_num"="1");

CREATE TABLE IF NOT EXISTS employees_landing (
  EmployeeID INT NOT NULL,
  LastName VARCHAR(255),
  FirstName VARCHAR(255),
  BirthDate DATE,
  Photo VARCHAR(255),
  Notes VARCHAR(5000),
  updated_at DATETIME
)
PRIMARY KEY(EmployeeID)
DISTRIBUTED BY HASH(EmployeeID) BUCKETS 4
PROPERTIES ("replication_num"="1");

CREATE TABLE IF NOT EXISTS products_landing (
  ProductID INT NOT NULL,
  ProductName VARCHAR(255),
  SupplierID INT,
  CategoryID INT,
  Unit VARCHAR(255),
  Price DECIMAL(10,2),
  updated_at DATETIME
)
PRIMARY KEY(ProductID)
DISTRIBUTED BY HASH(ProductID) BUCKETS 4
PROPERTIES ("replication_num"="1");

CREATE TABLE IF NOT EXISTS orders_landing (
  OrderID INT NOT NULL,
  CustomerID INT,
  EmployeeID INT,
  OrderDate DATE,
  ShipperID INT,
  updated_at DATETIME
)
PRIMARY KEY(OrderID)
DISTRIBUTED BY HASH(OrderID) BUCKETS 4
PROPERTIES ("replication_num"="1");

CREATE TABLE IF NOT EXISTS orderdetails_landing (
  OrderDetailID INT NOT NULL,
  OrderID INT,
  ProductID INT,
  Quantity INT,
  updated_at DATETIME
)
PRIMARY KEY(OrderDetailID)
DISTRIBUTED BY HASH(OrderID) BUCKETS 4
PROPERTIES ("replication_num"="1");