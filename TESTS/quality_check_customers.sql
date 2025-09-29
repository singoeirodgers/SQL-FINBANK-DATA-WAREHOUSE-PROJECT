/**********************************************************************************************
    Script: quality_check_customers.sql
    Purpose:
        Perform data quality checks on bronze.customers before loading into silver.customers,
        and verify data quality after insertion into silver layer.

    Layer Scope:
        - Pre-load validation on bronze.customers
        - Post-load validation on silver.customers

    Expectation:
        All checks should return ZERO ROWS unless data quality issues exist.

    Author: SINGOEI RODGERS
    Last Modified: <Insert Date>
**********************************************************************************************/

----------------------------------------------------------------------------------------------
-- 1. QUALITY CHECK BEFORE INSERTING INTO THE SILVER LAYER (bronze.customers)
----------------------------------------------------------------------------------------------

/*
========================================
CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY (customer_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    customer_id,
    COUNT(*) AS occurrence_count
FROM bronze.customers
GROUP BY customer_id
HAVING COUNT(*) > 1 OR customer_id IS NULL;


/*
========================================
CHECK FOR UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT *
FROM bronze.customers
WHERE
    customer_id        != TRIM(customer_id) OR
    first_name         != TRIM(first_name) OR
    last_name          != TRIM(last_name) OR
    email              != TRIM(email) OR
    phone              != TRIM(phone) OR
    address            != TRIM(address) OR
    city               != TRIM(city) OR
    state              != TRIM(state) OR
    zip_code           != TRIM(zip_code) OR
    employment_status  != TRIM(employment_status) OR
    branch_id          != TRIM(branch_id);


/*
========================================
CHECK FOR DATA STANDARDIZATION & CONSISTENCY
========================================
Review unique values to confirm normalization rules
*/
SELECT DISTINCT employment_status FROM bronze.customers;


----------------------------------------------------------------------------------------------
-- 2. VERIFY DATA QUALITY AFTER INSERTING INTO THE SILVER LAYER (silver.customers)
----------------------------------------------------------------------------------------------

/*
========================================
VERIFY NO NULLS OR DUPLICATES IN PRIMARY KEY (customer_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    customer_id,
    COUNT(*) AS occurrence_count
FROM silver.customers
GROUP BY customer_id
HAVING COUNT(*) > 1 OR customer_id IS NULL;


/*
========================================
VERIFY NO UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT *
FROM silver.customers
WHERE
    customer_id        != TRIM(customer_id) OR
    first_name         != TRIM(first_name) OR
    last_name          != TRIM(last_name) OR
    email              != TRIM(email) OR
    phone              != TRIM(phone) OR
    address            != TRIM(address) OR
    city               != TRIM(city) OR
    state              != TRIM(state) OR
    zip_code           != TRIM(zip_code) OR
    employment_status  != TRIM(employment_status) OR
    branch_id          != TRIM(branch_id);


/*
========================================
VERIFY DATA STANDARDIZATION & CONSISTENCY
========================================
Review unique values to confirm normalization rules
*/
SELECT DISTINCT employment_status FROM silver.customers;
