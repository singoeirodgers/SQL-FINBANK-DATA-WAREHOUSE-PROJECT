/**********************************************************************************************
    Script: quality_check_accounts.sql
    Purpose:
        Perform data quality checks on bronze.accounts before loading into silver.accounts,
        and verify data quality after insertion into silver layer.

    Layer Scope:
        - Pre-load validation on bronze.accounts
        - Post-load validation on silver.accounts

    Expectation:
        All checks should return ZERO ROWS unless data quality issues exist.

    Author: SINGOEI RODGERS
    Date: 29/09/2025
**********************************************************************************************/

----------------------------------------------------------------------------------------------
-- 1. QUALITY CHECK BEFORE INSERTING INTO THE SILVER LAYER (bronze.accounts)
----------------------------------------------------------------------------------------------

/*
========================================
CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY (account_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    account_id,
    COUNT(*) AS occurrence_count
FROM bronze.accounts
GROUP BY account_id
HAVING COUNT(*) > 1 OR account_id IS NULL;


/*
========================================
CHECK FOR UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT
    account_type,
    status
FROM bronze.accounts
WHERE 
    account_type != TRIM(account_type) 
    OR status != TRIM(status);


/*
========================================
CHECK FOR DATA STANDARDIZATION & CONSISTENCY
========================================
Review unique values to confirm normalization rules
*/
SELECT DISTINCT account_type FROM bronze.accounts;
SELECT DISTINCT status FROM bronze.accounts;


----------------------------------------------------------------------------------------------
-- 2. VERIFY DATA QUALITY AFTER INSERTING INTO THE SILVER LAYER (silver.accounts)
----------------------------------------------------------------------------------------------

/*
========================================
VERIFY NO NULLS OR DUPLICATES IN PRIMARY KEY (account_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    account_id,
    COUNT(*) AS occurrence_count
FROM silver.accounts
GROUP BY account_id
HAVING COUNT(*) > 1 OR account_id IS NULL;


/*
========================================
VERIFY NO UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT
    account_type,
    status
FROM silver.accounts
WHERE 
    account_type != TRIM(account_type) 
    OR status != TRIM(status);


/*
========================================
VERIFY DATA STANDARDIZATION & CONSISTENCY
========================================
Review unique values to confirm normalization rules
*/
SELECT DISTINCT account_type FROM silver.accounts;
SELECT DISTINCT status FROM silver.accounts;
