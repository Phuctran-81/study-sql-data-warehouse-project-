/* 
====================================
Create Database and Schemas
====================================
Script purpose:
	This script creates a new database named 'Datawarehouse'after checking if it already exists.
	ID the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
	within the database: 'bronze', 'silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'Datawarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution 
	and ensure you have proper backups before running this script.
*/

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN 
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datawarehouse;
END;
GO

-- Using Master for 'CREATE DATABASE'
CREATE DATABASE Datawarehouse;
GO

-- Switch to Datawarehouse for building Datawarehouse
--Step 1: Create schemas
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO
