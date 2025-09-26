/********************************************************************************************
 Script:     create_database.sql
 Purpose:    Creates a new SQL Server database called 'FinBankWarehouse' and sets up 
             three schemas: bronze, silver, and gold, following the modern data warehouse 
             layering approach.

 Description:
   - If the database already exists, it will be dropped and recreated. 
   - The 'bronze' schema is intended for raw ingested data (e.g., CSV bulk loads).
   - The 'silver' schema is intended for cleansed and standardized data.
   - The 'gold' schema is intended for analytics-ready data marts, fact, and dimension tables.

 WARNING:
   ⚠️ Running this script will permanently DROP the existing 'FinBankWarehouse' database 
   if it already exists. All objects and data inside it will be lost.
   Ensure you have backups or that it is safe to recreate the database before executing.

 Author: SINGOEI RODGERS
 Created:    26/09/2025
********************************************************************************************/

USE master;
GO

-- DROP AND RECREATE THE 'FinBankWarehouse' DATABASE
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'FinBankWarehouse')
BEGIN
	ALTER DATABASE FinBankWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE FinBankWarehouse;
END;
GO

-- CREATE THE 'FinBankWarehouse' DATABASE
CREATE DATABASE FinBankWarehouse;
GO
USE FinBankWarehouse;
GO

-- CREATE THE SCHEMAS
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
