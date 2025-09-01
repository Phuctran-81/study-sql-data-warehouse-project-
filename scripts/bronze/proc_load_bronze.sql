/*
===============================================================
Stored Procedure: Load Bronze Layer (source -> Bronze)
===============================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' schema from external CSV files.
	It performs the following actions:
	- Truncates the bronze tables before loading data
	- Use the `BULK INSERT` command to load data from csv files to bronze tables.

Parameter:
	 None.
	This stored procedure does not accept any prameters or return any values.
Usage example:
	 EXEC bronze.load_bronze;
===============================================================
*/


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
