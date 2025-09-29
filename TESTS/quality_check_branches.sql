/**********************************************************************************************
    Script: quality_check_branches.sql
    Purpose:
        Perform data quality checks on bronze.branches before loading into silver.branches,
        and verify data quality after insertion into silver layer.

    Layer Scope:
        - Pre-load validation on bronze.branches
        - Post-load validation on silver.branches

    Expectation:
        All checks should return ZERO ROWS unless data quality issues exist.

    Author: SINGOEI RODGERS
    Date: 29/09/2025
**********************************************************************************************/

----------------------------------------------------------------------------------------------
-- 1. QUALITY CHECK BEFORE INSERTING INTO THE SILVER LAYER (bronze.branches)
----------------------------------------------------------------------------------------------

/*
========================================
CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY (branch_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    branch_id,
    COUNT(*) AS occurrence_count
FROM bronze.branches
GROUP BY branch_id
HAVING COUNT(*) > 1 OR branch_id IS NULL;


/*
========================================
CHECK FOR UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT
    branch_name,
    city,
    state,
    zip_code
FROM bronze.branches
WHERE 
    branch_name != TRIM(branch_name)
    OR city != TRIM(city)
    OR state != TRIM(state)
    OR zip_code != TRIM(zip_code);


/*
========================================
CHECK FOR NULLS OR NEGATIVE VALUES IN NUMERIC COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT
    total_deposits,
    employee_count
FROM bronze.branches
WHERE 
    total_deposits < 0 
    OR employee_count < 0 
    OR total_deposits IS NULL 
    OR employee_count IS NULL;


----------------------------------------------------------------------------------------------
-- 2. VERIFY DATA QUALITY AFTER INSERTING INTO THE SILVER LAYER (silver.branches)
----------------------------------------------------------------------------------------------

/*
========================================
VERIFY NO NULLS OR DUPLICATES IN PRIMARY KEY (branch_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    branch_id,
    COUNT(*) AS occurrence_count
FROM silver.branches
GROUP BY branch_id
HAVING COUNT(*) > 1 OR branch_id IS NULL;


/*
========================================
VERIFY NO UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT
    branch_name,
    city,
    state,
    zip_code
FROM silver.branches
WHERE 
    branch_name != TRIM(branch_name)
    OR city != TRIM(city)
    OR state != TRIM(state)
    OR zip_code != TRIM(zip_code);


/*
========================================
VERIFY NO NULLS OR NEGATIVE VALUES IN NUMERIC COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT
    total_deposits,
    employee_count
FROM silver.branches
WHERE 
    total_deposits < 0 
    OR employee_count < 0 
    OR total_deposits IS NULL 
    OR employee_count IS NULL;
