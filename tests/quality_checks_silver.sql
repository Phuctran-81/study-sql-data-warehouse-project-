/*
===================================================================================
Quality checks
===================================================================================
Script Purpose:
    This scrpit peforms various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===================================================================================
*/
