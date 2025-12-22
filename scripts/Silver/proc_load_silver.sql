/*
=================================================================================
Stored Procedure: LOad Silver Layer (Bronze -> Silver)
=================================================================================
Scipt purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
    Actions Performed:
        - Truncates Silver tables.
        - Inserts transformed and cleansed data from Bronze into Silver tables.
Parameters:
    None.
      This store procedure does not accept any parameters or return any values.
Usage example:
    EXEC silver.load_Silver;
=================================================================================
*/

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @end_batch_time DATETIME;
	BEGIN TRY
		SET @start_batch_time = GETDATE ();
		PRINT '====================================';
		PRINT 'Loading Silver Layer';
		PRINT '====================================';

		PRINT '====================================';
		PRINT 'Loading CRM table'
		PRINT '====================================';

/*
====================================================
CLEANSING DATA AND INSERT INTO silver.crm_prd_info
====================================================
*/
SET @start_time = GETDATE ();
PRINT '>> Truncating Table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;

PRINT '>> Insert Data Into: silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
	 prd_id,
	 cat_id,
	 prd_key,
	 prd_nm,
	 prd_cost,
	 prd_line,
	 prd_start_dt,
	 prd_end_dt 
	)
SELECT
prd_id,
REPLACE (SUBSTRING (prd_key,1,5),'-','_') cat_id,
SUBSTRING (prd_key, CHARINDEX('-', prd_key, CHARINDEX ('-',prd_key)+1)+1, LEN (prd_key)) AS prd_key,
prd_nm,
ISNULL (prd_cost,0) prd_cost,
CASE WHEN prd_line = 'S' THEN 'Other Sales'
	 WHEN prd_line = 'R' THEN 'Road'
	 WHEN prd_line = 'T' THEN 'Touring'
	 WHEN prd_line = 'M' THEN 'Mountain'
	 ELSE 'n/a'
END AS prd_line,
prd_start_dt,
DATEADD (DAY, -1, LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info

SET @end_time = GETDATE ();
PRINT '>> Load Duration: ' + CAST (DATEDIFF (SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>>------------------';

/*
====================================================
CLEANSING DATA AND INSERT INTO silver.crm_cust_info
====================================================
*/
SET @start_time = GETDATE ();
PRINT '>> Truncating Table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;

PRINT '>> Insert Data Into silver.crm_cust_info';
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) cst_firstname,
TRIM (cst_lastname) cst_lastname,
CASE WHEN cst_marital_status = 'S' THEN 'Single'
	 WHEN cst_marital_status = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN cst_gndr = 'F' THEN 'Female'
	 WHEN cst_gndr = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (SELECT *,
ROW_NUMBER () OVER (PARTITION BY cst_key ORDER BY cst_create_date DESC) rankk
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL)t
WHERE rankk = 1

SET @end_time = GETDATE ();
PRINT '>> Load Duration: ' + CAST (DATEDIFF (SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>>--------------------';

/*
=========================================================
CLEANSING DATA AND INSERT INTO silver.crm_sales_details
=========================================================
*/
SET @start_time = GETDATE ();
PRINT '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;

PRINT '>> Insert Data Into: silver.crm_sales_details';
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN LEN (sls_order_dt) = 8 THEN CAST (CAST (sls_order_dt AS NVARCHAR) AS DATE)
END sls_order_dt,
CAST (CAST(sls_ship_dt AS NVARCHAR) AS DATE) sls_ship_dt,
CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE) sls_due_dt,
CASE WHEN sls_sales != abs (sls_quantity * sls_price) THEN abs (sls_price * sls_quantity) ELSE sls_sales
END sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL THEN ABS (sls_sales/sls_quantity) ELSE ABS(sls_price)
END AS sls_price
FROM bronze.crm_sales_details

SET @end_time = GETDATE ();
PRINT '>> Load Duration: ' + CAST (DATEDIFF (SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>>--------------------';
/*
=====================================================
CLEANSING DATA AND INSERT INTO silver.erp_cust_az12
=====================================================
*/
SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;

PRINT '>> Insert Data Into: silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
	)
SELECT 
CASE WHEN LEN (cid) = 13 THEN SUBSTRING (cid, 4, LEN (cid)) ELSE cid
END cid,
CASE WHEN bdate < '2014-01-29' THEN bdate
END bdate,
CASE WHEN gen IN ('F', 'Female') THEN 'Female'
	 WHEN gen IN ('M', 'Male') THEN 'Male'
	 ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12

SET @end_time = GETDATE ();
PRINT '>> Load Duration: ' + CAST(DATEDIFF (SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>>------------------';
/*
====================================================
CLEANSING DATA AND INSERT INTO silver.erp_loc_a101
====================================================
*/
SET @start_time = GETDATE ();
PRINT '>> Truncating Table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;

PRINT '>> Insert Data Into: silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
	)
SELECT 
REPLACE (cid, '-','') cid,
CASE WHEN cntry IN ('USA', 'US') THEN 'United States'
	 WHEN cntry IN ('DE', 'Germany') THEN 'Germany'
	 WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
	 ELSE cntry
END cntry
FROM bronze.erp_loc_a101

SET @end_time = GETDATE ();
PRINT 'Loading Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
PRINT '>>---------------'

/*
======================================================
CLEANSING DATA AND INSERT INTO silver.erp_px_cat_g1v2
======================================================
*/
SET @start_time = GETDATE ();
PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;

PRINT 'Load Data Into: silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
	)
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2

SET @end_time = GETDATE ();
PRINT '>> Load Duration: ' + CAST(DATEDIFF (SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>>------------------';

SET @end_batch_time = GETDATE ();
PRINT '==================================================';
PRINT '>> Loading Silver Layer is Completed';
PRINT 'ToTal Load Duration: ' + CAST (DATEDIFF (SECOND, @start_batch_time, @end_batch_time) AS NVARCHAR) + 'seconds';
PRINT '==================================================';
	END TRY
	BEGIN CATCH
		PRINT '===========================================';
		PRINT 'ERROR OCCURED DURING LOADIGNG SILVER LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE ();
		PRINT 'Error Message: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST (ERROR_STATE () AS NVARCHAR);
		PRINT '===========================================';
	END CATCH 
END




