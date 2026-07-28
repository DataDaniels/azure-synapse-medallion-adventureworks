CREATE DATABASE GoldAnalytics;

-- 1. Garanta que está executando no seu banco de dados analítico
USE GoldAnalytics;
GO

-- ---------------------------------------------------------------------
-- View: dimCustomer
-- ---------------------------------------------------------------------
IF OBJECT_ID('dimCustomer', 'V') IS NOT NULL DROP VIEW dimCustomer;
GO

CREATE VIEW dimCustomer AS
SELECT *
FROM OPENROWSET(
    BULK 'https://adlsposgraduacaodaniel.dfs.core.windows.net/curated/dimCustomer/',
    FORMAT = 'DELTA'
) AS [result];
GO

-- ---------------------------------------------------------------------
-- View: dimProduct
-- ---------------------------------------------------------------------
IF OBJECT_ID('dimProduct', 'V') IS NOT NULL DROP VIEW dimProduct;
GO

CREATE VIEW dimProduct AS
SELECT *
FROM OPENROWSET(
    BULK 'https://adlsposgraduacaodaniel.dfs.core.windows.net/curated/dimProduct/',
    FORMAT = 'DELTA'
) AS [result];
GO

-- ---------------------------------------------------------------------
-- View: dimDate
-- ---------------------------------------------------------------------
IF OBJECT_ID('dimDate', 'V') IS NOT NULL DROP VIEW dimDate;
GO

CREATE VIEW dimDate AS
SELECT *
FROM OPENROWSET(
    BULK 'https://adlsposgraduacaodaniel.dfs.core.windows.net/curated/dimDate/',
    FORMAT = 'DELTA'
) AS [result];
GO

-- ---------------------------------------------------------------------
-- View: factSales
-- ---------------------------------------------------------------------
IF OBJECT_ID('factSales', 'V') IS NOT NULL DROP VIEW factSales;
GO

CREATE VIEW factSales AS
SELECT *
FROM OPENROWSET(
    BULK 'https://adlsposgraduacaodaniel.dfs.core.windows.net/curated/factSales/',
    FORMAT = 'DELTA'
) AS [result];
GO