-- mysql/init.sql
CREATE DATABASE IF NOT EXISTS order_management;
USE order_management;


-- Core tables 
CREATE TABLE IF NOT EXISTS Customers (
  CustomerID INT PRIMARY KEY,
  CustomerName VARCHAR(255),
  ContactName VARCHAR(255),
  Address VARCHAR(255),
  City VARCHAR(100),
  PostalCode VARCHAR(50),
  Country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS Categories (
  CategoryID INT PRIMARY KEY,
  CategoryName VARCHAR(255),
  Description VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS Suppliers (
  SupplierID INT PRIMARY KEY,
  SupplierName VARCHAR(255),
  ContactName VARCHAR(255),
  Address VARCHAR(255),
  City VARCHAR(100),
  PostalCode VARCHAR(50),
  Country VARCHAR(100),
  Phone VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Shippers (
  ShipperID INT PRIMARY KEY,
  ShipperName VARCHAR(255),
  Phone VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Employees (
  EmployeeID INT PRIMARY KEY,
  LastName VARCHAR(255),
  FirstName VARCHAR(255),
  BirthDate DATE,
  Photo VARCHAR(255),
  Notes TEXT
);

CREATE TABLE IF NOT EXISTS Products (
  ProductID INT PRIMARY KEY,
  ProductName VARCHAR(255),
  SupplierID INT,
  CategoryID INT,
  Unit VARCHAR(255),
  Price DECIMAL(10,2),
  INDEX idx_products_supplier (SupplierID),
  INDEX idx_products_category (CategoryID)
);

CREATE TABLE IF NOT EXISTS Orders (
  OrderID INT PRIMARY KEY,
  CustomerID INT,
  EmployeeID INT,
  OrderDate DATE,
  ShipperID INT,
  INDEX idx_orders_customer (CustomerID),
  INDEX idx_orders_employee (EmployeeID),
  INDEX idx_orders_shipper (ShipperID)
);

CREATE TABLE IF NOT EXISTS OrderDetails (
  OrderDetailID INT PRIMARY KEY,
  OrderID INT,
  ProductID INT,
  Quantity INT,
  INDEX idx_od_order (OrderID),
  INDEX idx_od_product (ProductID)
);

-- -------------------------
-- Seed data (small but usable)
-- -------------------------
INSERT IGNORE INTO Categories VALUES
(1,'Beverages','Soft drinks, coffees, teas'),
(2,'Condiments','Sweet and savory sauces'),
(3,'Confections','Desserts, candies');

INSERT IGNORE INTO Suppliers VALUES
(1,'Exotic Liquid','Charlotte Cooper','49 Gilbert St.','Londona','EC1 4SD','UK','(171) 555-2222'),
(2,'New Orleans Cajun Delights','Shelley Burke','P.O. Box 78934','New Orleans','70117','USA','(100) 555-4822');

INSERT IGNORE INTO Shippers VALUES
(1,'Speedy Express','(503) 555-9831'),
(2,'United Package','(503) 555-3199');

INSERT IGNORE INTO Employees VALUES
(1,'Davolio','Nancy','1968-12-08','EmpID1.pic','BA in psychology...'),
(2,'Fuller','Andrew','1952-02-19','EmpID2.pic','Ph.D. in marketing...');

INSERT IGNORE INTO Customers VALUES
(1,'Alfreds Futterkiste','Maria Anders','Obere Str. 57','Berlin','12209','Germany'),
(2,'Around the Horn','Thomas Hardy','120 Hanover Sq.','London','WA1 1DP','UK'),
(3,'Bólido Comidas preparadas','Martín Sommer','C/ Araquil, 67','Madrid','28023','Spain');

INSERT IGNORE INTO Products VALUES
(1,'Chais',1,1,'10 boxes x 20 bags',18.00),
(2,'Chang',1,1,'24 - 12 oz bottles',19.00),
(3,'Aniseed Syrup',2,2,'12 - 550 ml bottles',10.00);

INSERT IGNORE INTO Orders VALUES
(10248,1,1,'1996-07-04',1),
(10249,2,2,'1996-07-05',2);

INSERT IGNORE INTO OrderDetails VALUES
(1,10248,1,12),
(2,10248,2,10),
(3,10249,3,5);

-- Add incremental column to all tables (MySQL 8 compatible)
ALTER TABLE Customers     ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE Categories    ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE Suppliers     ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE Shippers      ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE Employees     ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE Products      ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE Orders        ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE OrderDetails  ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

-- backfill (safe no-op if already has values)
UPDATE Customers    SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL;
UPDATE Categories   SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL;
UPDATE Suppliers    SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL;
UPDATE Shippers     SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL;
UPDATE Employees    SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL;
UPDATE Products     SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL;
UPDATE Orders       SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL;
UPDATE OrderDetails SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL;