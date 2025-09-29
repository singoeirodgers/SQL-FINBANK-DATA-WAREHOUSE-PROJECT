/**********************************************************************************************
    Script: quality_check_transactions.sql
    Purpose:
        Perform data quality checks on bronze.transactions before loading into silver.transactions,
        and verify data quality after insertion into silver layer.

    Layer Scope:
        - Pre-load validation on bronze.transactions
        - Post-load validation on silver.transactions

    Expectation:
        All checks should return ZERO ROWS unless data quality issues exist.

    Author: SINGOEI RODGERS
    Date: 29/09/2025
**********************************************************************************************/

----------------------------------------------------------------------------------------------
-- 1. QUALITY CHECK BEFORE INSERTING INTO THE SILVER LAYER (bronze.transactions)
----------------------------------------------------------------------------------------------

/*
========================================
CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY (transaction_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    transaction_id,
    COUNT(*) AS occurrence_count
FROM bronze.transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1 OR transaction_id IS NULL;


/*
========================================
CHECK FOR UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT *
FROM bronze.transactions
WHERE 
    TRIM(transaction_type) != transaction_type
    OR TRIM(merchant_name) != merchant_name
    OR TRIM(merchant_category) != merchant_category
    OR TRIM(description) != description
    OR TRIM(status) != status;


/*
========================================
CHECK FOR DATA STANDARDIZATION & CONSISTENCY
========================================
Review unique values to confirm normalization rules
*/
SELECT DISTINCT transaction_type FROM bronze.transactions;
SELECT DISTINCT merchant_category FROM bronze.transactions;
SELECT DISTINCT status FROM bronze.transactions;


----------------------------------------------------------------------------------------------
-- 2. VERIFY QUALITY CHECK AFTER INSERTING INTO THE SILVER LAYER (silver.transactions)
----------------------------------------------------------------------------------------------

/*
========================================
VERIFY NO NULLS OR DUPLICATES IN PRIMARY KEY (transaction_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    transaction_id,
    COUNT(*) AS occurrence_count
FROM silver.transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1 OR transaction_id IS NULL;


/*
========================================
VERIFY NO UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT *
FROM silver.transactions
WHERE 
    TRIM(transaction_type) != transaction_type
    OR TRIM(merchant_name) != merchant_name
    OR TRIM(merchant_category) != merchant_category
    OR TRIM(description) != description
    OR TRIM(status) != status;


/*
========================================
VERIFY DATA STANDARDIZATION & CONSISTENCY
========================================
Review unique values to confirm normalization rules
*/
SELECT DISTINCT transaction_type FROM silver.transactions;
SELECT DISTINCT merchant_category FROM silver.transactions;
SELECT DISTINCT status FROM silver.transactions;
