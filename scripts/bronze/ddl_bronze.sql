/* 
================================================================================
DDL Script: Create Bronze Tables
================================================================================
Script purpose:
	This Scrpit creates tables in the 'bronze' schema, dropping existing tables
	if they already exist.
	Run this script to re-define the DDL structure of 'bronze' Tables
================================================================================
*/


CREATE TABLE bronze.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR (50),
cst_marterial_status NVARCHAR (50),
cst_gndr NVARCHAR (50),
cst_create_date DATE
);

-- Drop table for Alter table by create new table and change the type data
IF OBJECT_ID ('bronze.crm_prd_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR (50),
prd_cost INT,
prd_line NVARCHAR (10),
prd_start_dt DATE,
pr_end_dt DATE
);

IF OBJECT_ID ('bronze.crm_sales_details' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
sls_ord_num NVARCHAR (50),
sls_prd_key NVARCHAR (50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

CREATE TABLE bronze.erp_cust_az12 (
cid NVARCHAR (50),
bdate DATE,
gen NVARCHAR (50)
);

CREATE TABLE bronze.erp_loc_a101 (
cid NVARCHAR (50),
cntry NVARCHAR (50)
);

CREATE TABLE bronze.erp_px_cat_g1v2 (
id NVARCHAR (50),
cat NVARCHAR (50),
subcat NVARCHAR (70),
maintenance NVARCHAR(20)
);


-- Create store procedure for frequently used SQL code 
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

	-- Track ETL duration: Helps to identify bottlenecks, optimize performance, monitor trends, detect issues
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	-- ADD TRY ... CATCH: for Ensuring error handling, data integrity, and issue logging for easier debugging.
	-- SQL runs the TRY block, and if it falls, it runs the CATCH block to handle the error.
	BEGIN TRY

		-- CREATE PRINT for following the execution
		PRINT '=======================================================';
		PRINT 'Loading bronze layer';
		PRINT '=======================================================';

		PRINT '-------------------------------------------------------';
		PRINT 'Loading CRM table';
		PRINT '-------------------------------------------------------';

		SET @batch_start_time = GETDATE ();
		SET @start_time = GETDATE();
		-- TRUNCATE TABLE before BULK INSERT to avoid duplicate data
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;
		-- Insert full data from csv files to sql
		PRINT '>> Loading data into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\Setdata\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>----------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Loading data into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\Setdata\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '>>------------------------'

		SET @start_time = GETDATE ();
		PRINT '>> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> Loading data into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\Setdata\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>------------------------'

		PRINT '-------------------------------------------------------';
		PRINT 'Loading ERP table';
		PRINT '-------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>> Loading data into: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\Setdata\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Loading duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>> Loading data into: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\Setdata\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE ();
		PRINT '>> Loading duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'>>--------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>> Loading data into: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\Setdata\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE ();
		PRINT '>> Loading duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------------'
		SET @batch_end_time = GETDATE ()
		PRINT '>> Batch Loading Duration:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------------'


	END TRY
	BEGIN CATCH
		PRINT '============================================'
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error message' + ERROR_MESSAGE ();
		PRINT 'Error message' + ERROR_NUMBER ();
		PRINT 'Error message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================================'
	END CATCH
END;
