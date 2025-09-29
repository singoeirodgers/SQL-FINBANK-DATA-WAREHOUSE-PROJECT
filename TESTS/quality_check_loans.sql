/**********************************************************************************************
    Script: quality_check_loans.sql
    Purpose:
        Perform data quality checks on bronze.loans before loading into silver.loans,
        and verify data quality after insertion into silver layer.

    Layer Scope:
        - Pre-load validation on bronze.loans
        - Post-load validation on silver.loans

    Expectation:
        All checks should return ZERO ROWS unless data quality issues exist.

    Author: SINGOEI RODGERS
    Date: 29/09/2025
**********************************************************************************************/

----------------------------------------------------------------------------------------------
-- 1. QUALITY CHECK BEFORE INSERTING INTO THE SILVER LAYER (bronze.loans)
----------------------------------------------------------------------------------------------

/*
========================================
CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY (loan_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    loan_id,
    COUNT(*) AS occurrence_count
FROM bronze.loans
GROUP BY loan_id
HAVING COUNT(*) > 1 OR loan_id IS NULL;


/*
========================================
CHECK FOR UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT *
FROM bronze.loans
WHERE 
    TRIM(loan_type) != loan_type
    OR TRIM(status) != status;


/*
========================================
CHECK FOR DATA STANDARDIZATION & CONSISTENCY
========================================
Review unique values to confirm normalization rules
*/
SELECT DISTINCT loan_type FROM bronze.loans;
SELECT DISTINCT status FROM bronze.loans;


----------------------------------------------------------------------------------------------
-- 2. VERIFY QUALITY CHECK AFTER INSERTING INTO THE SILVER LAYER (silver.loans)
----------------------------------------------------------------------------------------------

/*
========================================
VERIFY NO NULLS OR DUPLICATES IN PRIMARY KEY (loan_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    loan_id,
    COUNT(*) AS occurrence_count
FROM silver.loans
GROUP BY loan_id
HAVING COUNT(*) > 1 OR loan_id IS NULL;


/*
========================================
VERIFY NO UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT *
FROM silver.loans
WHERE 
    TRIM(loan_type) != loan_type
    OR TRIM(status) != status;


/*
========================================
VERIFY DATA STANDARDIZATION & CONSISTENCY
========================================
Review unique values to confirm normalization rules
*/
SELECT DISTINCT loan_type FROM silver.loans;
SELECT DISTINCT status FROM silver.loans;
