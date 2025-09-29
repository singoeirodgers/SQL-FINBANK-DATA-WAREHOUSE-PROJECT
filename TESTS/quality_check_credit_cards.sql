/**********************************************************************************************
    Script: quality_check_credit_cards.sql
    Purpose:
        Perform data quality checks on bronze.credit_cards before loading into silver.credit_cards,
        and verify data quality after insertion into silver layer.

    Layer Scope:
        - Pre-load validation on bronze.credit_cards
        - Post-load validation on silver.credit_cards

    Expectation:
        All checks should return ZERO ROWS unless data quality issues exist.

    Author: SINGOEI RODGERS
    Date: 29/09/2025
**********************************************************************************************/

----------------------------------------------------------------------------------------------
-- 1. QUALITY CHECK BEFORE INSERTING INTO THE SILVER LAYER (bronze.credit_cards)
----------------------------------------------------------------------------------------------

/*
========================================
CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY (card_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    card_id,
    COUNT(*) AS occurrence_count
FROM bronze.credit_cards
GROUP BY card_id
HAVING COUNT(*) > 1 OR card_id IS NULL;


/*
========================================
CHECK FOR UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT *
FROM bronze.credit_cards
WHERE 
    card_id      != TRIM(card_id) OR
    customer_id  != TRIM(customer_id) OR
    card_number  != TRIM(card_number) OR
    card_type    != TRIM(card_type) OR
    status       != TRIM(status);


/*
========================================
CHECK FOR DATA STANDARDIZATION & CONSISTENCY
========================================
Review unique values to confirm normalization rules
*/
SELECT DISTINCT card_type FROM bronze.credit_cards;
SELECT DISTINCT status    FROM bronze.credit_cards;


/*
========================================
CHECK FOR NULLS OR NEGATIVE VALUES IN NUMERIC COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT
    credit_limit,
    current_balance,
    available_credit
FROM bronze.credit_cards
WHERE 
    credit_limit      < 0 OR
    current_balance   < 0 OR
    available_credit  < 0 OR
    credit_limit      IS NULL OR
    current_balance   IS NULL OR
    available_credit  IS NULL;


/*
========================================
CHECK BUSINESS RULE: AVAILABLE CREDIT = CREDIT LIMIT - CURRENT BALANCE
========================================
Expectation: NO RESULT
*/
SELECT *
FROM bronze.credit_cards
WHERE available_credit <> credit_limit - current_balance;


/*
========================================
CHECK BUSINESS RULE: EXPIRY DATE SHOULD BE AFTER ISSUE DATE
========================================
Expectation: NO RESULT
*/
SELECT *
FROM bronze.credit_cards
WHERE expiry_date <= issue_date;


----------------------------------------------------------------------------------------------
-- 2. VERIFY DATA QUALITY AFTER INSERTING INTO THE SILVER LAYER (silver.credit_cards)
----------------------------------------------------------------------------------------------

/*
========================================
VERIFY NO NULLS OR DUPLICATES IN PRIMARY KEY (card_id)
========================================
Expectation: NO RESULT
*/
SELECT 
    card_id,
    COUNT(*) AS occurrence_count
FROM silver.credit_cards
GROUP BY card_id
HAVING COUNT(*) > 1 OR card_id IS NULL;


/*
========================================
VERIFY NO UNWANTED SPACES IN STRING COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT *
FROM silver.credit_cards
WHERE 
    card_id      != TRIM(card_id) OR
    customer_id  != TRIM(customer_id) OR
    card_number  != TRIM(card_number) OR
    card_type    != TRIM(card_type) OR
    status       != TRIM(status);


/*
========================================
VERIFY DATA STANDARDIZATION & CONSISTENCY
========================================
Review unique values to confirm normalization rules
*/
SELECT DISTINCT card_type FROM silver.credit_cards;
SELECT DISTINCT status    FROM silver.credit_cards;


/*
========================================
VERIFY NO NULLS OR NEGATIVE VALUES IN NUMERIC COLUMNS
========================================
Expectation: NO RESULT
*/
SELECT
    credit_limit,
    current_balance,
    available_credit
FROM silver.credit_cards
WHERE 
    credit_limit      < 0 OR
    current_balance   < 0 OR
    available_credit  < 0 OR
    credit_limit      IS NULL OR
    current_balance   IS NULL OR
    available_credit  IS NULL;


/*
========================================
VERIFY BUSINESS RULE: AVAILABLE CREDIT = CREDIT LIMIT - CURRENT BALANCE
========================================
Expectation: NO RESULT
*/
SELECT *
FROM silver.credit_cards
WHERE available_credit <> credit_limit - current_balance;


/*
========================================
VERIFY BUSINESS RULE: EXPIRY DATE SHOULD BE AFTER ISSUE DATE
========================================
Expectation: NO RESULT
*/
SELECT *
FROM silver.credit_cards
WHERE expiry_date <= issue_date;
