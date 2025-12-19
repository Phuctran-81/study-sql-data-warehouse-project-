/*
=====================================================================================================
DDL Script: Create Bronze Tables
=====================================================================================================
Script Purpose:
  This script creates tables in the 'bronze' schema, dropping existing tables if they already exist.
  Run this script to re-define the DDL structure of 'bronze' Tables
=====================================================================================================
*/
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
  DROP TABLE bronze.crm_cust_info;
GO

