
/*
==========================================
Create Database and Schema
==========================================
Script Purpose:
    This screipt creates a new database named "DATAWAREHOUSE" after checking if it already exsits. Additional script sets up the three schemas 
    within the database: 'bronze', 'silver', 'gold'

Warning: Running script will drop the entire 'DATAWAREHOUSE' database if it exists. ALL data in database will be permanetly deleted.

*/



USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
