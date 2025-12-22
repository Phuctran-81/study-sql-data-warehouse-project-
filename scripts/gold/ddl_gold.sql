/*
==================================================================================
DDL Script: Create Gold Views
==================================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.
Usage:
    - These views can be queried directly for analytics and reporting.
==================================================================================
*/
/*
========================================================
CREATE DIMENSION: gold.dim_customers
========================================================
*/
IF OBJECT_ID ('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER () OVER (ORDER BY cst_id) AS customer_key,
i.cst_id							 AS customer_id,
i.cst_key							 AS customer_number,
i.cst_firstname						 AS first_name,
i.cst_lastname						 AS last_name,
i.cst_marital_status				 AS marital_status,
CASE WHEN i.cst_gndr = 'n/a' THEN a.gen ELSE i.cst_gndr
							END		 AS gender,
a.bdate								 AS birth_date,
l.cntry								 AS country,
i.cst_create_date					 AS create_date
FROM silver.crm_cust_info i
LEFT JOIN silver.erp_cust_az12 a
ON i.cst_key = a.cid
LEFT JOIN silver.erp_loc_a101 l
ON i.cst_key = l.cid ;
GO

/*
========================================================
CREATE DIMENSION: gold.dim_products
========================================================
*/
IF OBJECT_ID ('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS 
SELECT
ROW_NUMBER () OVER (ORDER BY prd_start_dt, prd_key) AS product_key,
i.prd_id		AS product_id,
i.prd_key		AS product_number,
i.prd_nm		AS product_name,
i.cat_id		AS category_id,
c.cat			AS category_name,
c.subcat		AS subcategory_name,
i.prd_cost		AS product_cost,
i.prd_line		AS product_line,
c.maintenance	AS maintenance,
i.prd_start_dt  AS product_start_date
FROM silver.crm_prd_info i
LEFT JOIN silver.erp_px_cat_g1v2 c
ON i.cat_id = c.id
WHERE prd_end_dt IS NULL ;
GO

/*
========================================================
CREATE FACT TABLE: gold.fact_sales
========================================================
*/
IF OBJECT_ID ('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
s.sls_ord_num		AS order_number,
c.customer_key		AS customer_key,
p.product_key		AS product_key,
s.sls_order_dt		AS order_date,
s.sls_ship_dt		AS shipping_date,
s.sls_due_dt		AS due_date,
s.sls_sales          AS sales_amount,
s.sls_quantity		AS quantity,
s.sls_price			AS price
FROM silver.crm_sales_details s
LEFT JOIN gold.dim_customers c
ON c.customer_id = s.sls_cust_id
LEFT JOIN gold.dim_products p
ON s.sls_prd_key = p.product_number;
GO
