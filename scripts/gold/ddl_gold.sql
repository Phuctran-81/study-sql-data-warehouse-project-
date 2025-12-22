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
cst_id								 AS customer_id,
cst_key								 AS customer_number,
cst_firstname						 AS first_name,
cst_lastname						 AS last_name,
cst_marital_status					 AS marital_status,
CASE WHEN cst_gndr = 'n/a' THEN gen ELSE cst_gndr
							END		 AS gender,
bdate								 AS birth_date,
cntry								 AS country,
cst_create_date						 AS create_date
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
prd_id			AS product_id,
prd_key			AS product_number,
prd_nm			AS product_name,
cat_id		    AS category_id,
c.cat			AS category_name,
c.subcat		AS subcategory_name,
prd_cost		AS product_cost,
prd_line		AS product_line,
maintenance		AS maintenance,
prd_start_dt    AS product_start_date
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
sls_ord_num			AS order_number,
c.customer_key		AS customer_key,
p.product_key		AS product_key,
sls_order_dt		AS order_date,
sls_ship_dt			AS shipping_date,
sls_due_dt			AS due_date,
sls_sales           AS sales_amount,
sls_quantity		AS quantity,
sls_price			AS price
FROM silver.crm_sales_details s
LEFT JOIN gold.dim_customers c
ON c.customer_id = s.sls_cust_id
LEFT JOIN gold.dim_products p
ON s.sls_prd_key = p.product_number;
GO
