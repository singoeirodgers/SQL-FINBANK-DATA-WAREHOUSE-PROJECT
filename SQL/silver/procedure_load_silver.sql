/**********************************************************************************************
    File: procedure_load_silver.sql
    Database: FinBankWarehouse
    Layer: Silver (Cleansed & Standardized Layer)

    Purpose:
        This stored procedure loads data from the Bronze layer into the Silver layer.
        It applies data cleaning, standardization, formatting, and transformations
        to ensure consistency and usability of the data downstream.

        Key Tasks Performed:
        - Truncate Silver tables before reloading
        - Insert cleansed/standardized data from Bronze tables
        - Apply transformations:
            * Normalize account types, statuses
            * Standardize branch/city/state/zip formatting
            * Cleanse and validate credit card balances & limits
            * Standardize phone numbers, SSNs, employment statuses
            * Standardize loan types and statuses
            * Normalize transaction types, merchant details, and statuses
        - Log durations for each step

    Author: SINGOEI RODGERS
    Created On: 29/09/2025
**********************************************************************************************/

--EXEC silver.load_silver;

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @batch_start_time DATETIME, @end_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        BEGIN TRANSACTION;

        PRINT '================================================================================================================';
        PRINT 'LOADING THE SILVER LAYER';
        PRINT '================================================================================================================';

        /*
        ===========================================================
        TRUNCATING & INSERTING INTO silver.accounts
        ===========================================================
        */
        PRINT '================================================================';
        PRINT 'BEGINNING TRUNCATING & INSERTING INTO silver.accounts';
        PRINT '================================================================';

        PRINT '----------------------------------------------------------------';
        SET @start_time = GETDATE();
        PRINT '>>> Truncating table silver.accounts';
        TRUNCATE TABLE silver.accounts;
        PRINT '>>> Inserting into silver.accounts';
        INSERT INTO silver.accounts (
            account_id,
            customer_id,
            account_type,
            account_number,
            current_balance,
            open_date,
            interest_rate,
            status
        )
        SELECT
            account_id,
            customer_id,
            -- Standardize account_type
            CASE WHEN UPPER (TRIM (account_type)) = 'SAVINGS' THEN 'Savings'
                 WHEN UPPER (TRIM (account_type)) = 'CD' THEN 'Certificate of Deposit'
                 WHEN UPPER (TRIM (account_type)) = 'MONEY MARKET' THEN 'Money Market'
                 WHEN UPPER (TRIM (account_type)) = 'CHECKING' THEN 'Checking'
                 ELSE 'Unknown'
            END AS account_type,
            account_number,
            current_balance,
            open_date,
            interest_rate,
            -- Standardize status
            CASE WHEN UPPER (TRIM (status)) = 'CLOSED' THEN 'Closed'
                 WHEN UPPER (TRIM (status)) = 'ACTIVE' THEN 'Active'
                 WHEN UPPER (TRIM (status)) = 'DORMANT' THEN 'Dormant'
                 ELSE 'Unknown'
            END AS status
        FROM (
            -- In case later there's a duplicate account_id, pick one with the most recent open_date
            SELECT *,
                   ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY open_date DESC) AS flag_last
            FROM bronze.accounts
        ) t;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '----------------------------------------------------------------';


        /*
        ===========================================================
        TRUNCATING & INSERTING INTO silver.branches
        ===========================================================
        */
        PRINT '================================================================';
        PRINT 'BEGINNING TRUNCATING & INSERTING INTO silver.branches';
        PRINT '================================================================';

        PRINT '----------------------------------------------------------------';
        SET @start_time = GETDATE();
        PRINT '>>> Truncating table silver.branches';
        TRUNCATE TABLE silver.branches;
        PRINT '>>> Inserting into silver.branches';
        INSERT INTO silver.branches(
            branch_id,
            branch_name,
            city,
            state,
            zip_code,
            latitude,
            longitude,
            opening_date,
            total_deposits,
            employee_count
        )
        SELECT 
            branch_id,
            TRIM(branch_name) AS branch_name,
            TRIM(city) AS city,
            TRIM(state) AS state,
            TRIM(zip_code) AS zip_code,
            latitude,
            longitude,
            opening_date,
            total_deposits,
            employee_count
        FROM bronze.branches;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '----------------------------------------------------------------';


        /*
        ===========================================================
        TRUNCATING & INSERTING INTO silver.credit_cards
        ===========================================================
        */
        PRINT '================================================================';
        PRINT 'BEGINNING TRUNCATING & INSERTING INTO silver.credit_cards';
        PRINT '================================================================';

        PRINT '----------------------------------------------------------------';
        SET @start_time = GETDATE();
        PRINT '>>> Truncating table silver.credit_cards';
        TRUNCATE TABLE silver.credit_cards;
        PRINT '>>> Inserting into silver.credit_cards';
        INSERT INTO silver.credit_cards(
            card_id,
            customer_id,
            card_number,
            expiry_date,
            credit_limit,
            current_balance,
            available_credit,
            issue_date,
            card_type,
            status
        )
        SELECT 
            card_id,
            customer_id,
            TRIM(card_number) AS card_number,
            expiry_date,
            CASE WHEN credit_limit <= 0 OR credit_limit IS NULL THEN 0 ELSE credit_limit END AS credit_limit,
            CASE WHEN current_balance <= 0 OR current_balance IS NULL THEN 0 ELSE current_balance END AS current_balance,
            CASE WHEN available_credit <= 0 OR available_credit IS NULL THEN 0 ELSE available_credit END AS available_credit,
            issue_date,
            TRIM(card_type) AS card_type,
            TRIM(status) AS status
        FROM bronze.credit_cards;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '----------------------------------------------------------------';


        /*
        ===========================================================
        TRUNCATING & INSERTING INTO silver.customers
        ===========================================================
        */
        PRINT '================================================================';
        PRINT 'BEGINNING TRUNCATING & INSERTING INTO silver.customers';
        PRINT '================================================================';

        PRINT '----------------------------------------------------------------';
        SET @start_time = GETDATE();
        PRINT '>>> Truncating table silver.customers';
        TRUNCATE TABLE silver.customers;
        PRINT '>>> Inserting into silver.customers';
        WITH phone_CTE AS (
            SELECT 
                customer_id,
                phone AS original_phone,
                -- Clean phone number (remove extension and punctuation)
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    CASE 
                        WHEN CHARINDEX('x', phone) > 0 
                             THEN LEFT(phone, CHARINDEX('x', phone) - 1)
                             ELSE phone
                    END, '.', ''), '(', ''), ')', ''), '-', ''), ' ', '') AS clean_digits_raw
            FROM bronze.customers
        ),
        phone_formatted AS (
            SELECT
                customer_id,
                original_phone,
                CASE 
                    WHEN clean_digits_raw LIKE '+1%' THEN SUBSTRING(clean_digits_raw, 3, LEN(clean_digits_raw) - 2)
                    WHEN clean_digits_raw LIKE '001%' THEN SUBSTRING(clean_digits_raw, 4, LEN(clean_digits_raw) - 3)
                    ELSE clean_digits_raw
                END AS clean_digits
            FROM phone_CTE
        ),
        final_phone AS (
            SELECT
                customer_id,
                original_phone,
                clean_digits,
                CASE 
                    WHEN LEN(clean_digits) = 10 
                         THEN '(' + LEFT(clean_digits, 3) + ') ' + SUBSTRING(clean_digits, 4, 3) + '-' + RIGHT(clean_digits, 4)
                         ELSE original_phone
                END AS formatted_phone
            FROM phone_formatted
        )
        INSERT INTO silver.customers(
            customer_id,
            first_name,
            last_name,
            email,
            phone,
            address,
            city,
            state,
            zip_code,
            date_of_birth,
            ssn,
            customer_since,
            credit_score,
            annual_income,
            employment_status,
            branch_id
        )
        SELECT 
            c.customer_id,
            TRIM(c.first_name) AS first_name,
            TRIM(c.last_name) AS last_name,
            TRIM(LOWER(c.email)) AS email,
            f.formatted_phone AS phone,
            TRIM(c.address) AS address,
            TRIM(c.city) AS city,
            TRIM(c.state) AS state,
            TRIM(c.zip_code) AS zip_code,
            c.date_of_birth,
            CASE 
                WHEN LEN(TRIM(CAST(ssn AS VARCHAR))) != 9 
                     THEN CAST(TRIM(CAST(ssn AS VARCHAR)) + '0' AS INT)
                     ELSE ssn
            END AS ssn,
            c.customer_since,
            c.credit_score,
            c.annual_income,
            CASE 
                WHEN LOWER(TRIM(c.employment_status)) LIKE 'retired' THEN 'Retired'
                WHEN LOWER(TRIM(c.employment_status)) LIKE 'self-employed' THEN 'Self Employed'
                WHEN LOWER(TRIM(c.employment_status)) LIKE 'employed' THEN 'Employed'
                ELSE 'Unemployed'
            END AS employment_status,
            c.branch_id
        FROM bronze.customers c
        LEFT JOIN final_phone f ON c.customer_id = f.customer_id;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '----------------------------------------------------------------';


        /*
        ===========================================================
        TRUNCATING & INSERTING INTO silver.loans
        ===========================================================
        */
        PRINT '================================================================';
        PRINT 'BEGINNING TRUNCATING & INSERTING INTO silver.loans';
        PRINT '================================================================';

        PRINT '----------------------------------------------------------------';
        SET @start_time = GETDATE();
        PRINT '>>> Truncating table silver.loans';
        TRUNCATE TABLE silver.loans;
        PRINT '>>> Inserting into silver.loans';
        INSERT INTO silver.loans(
            loan_id,
            customer_id,
            loan_type,
            loan_amount,
            interest_rate,
            term_months,
            start_date,
            monthly_payment,
            remaining_balance,
            status
        )
        SELECT loan_id,
               customer_id,
               CASE 
                    WHEN LOWER(TRIM(loan_type)) LIKE '%mortgage%' THEN 'Mortgage'
                    WHEN LOWER(TRIM(loan_type)) LIKE '%auto%' THEN 'Auto Loan'
                    WHEN LOWER(TRIM(loan_type)) LIKE '%personal%' THEN 'Personal Loan'
                    WHEN LOWER(TRIM(loan_type)) LIKE '%student%' THEN 'Student Loan'
                    ELSE 'Other'
               END AS loan_type,
               loan_amount,
               interest_rate,
               term_months,
               start_date,
               monthly_payment,
               CASE WHEN remaining_balance < 0 OR remaining_balance IS NULL THEN 0 ELSE remaining_balance END AS remaining_balance,
               CASE 
                    WHEN LOWER(TRIM(status)) LIKE '%current%' THEN 'Current'
                    WHEN LOWER(TRIM(status)) LIKE '%delinquent%' THEN 'Delinquent'
                    WHEN LOWER(TRIM(status)) LIKE '%paid off%' THEN 'Paid Off'
                    ELSE 'Unknown'
               END AS status
        FROM bronze.loans;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '----------------------------------------------------------------';


        /*
        ===========================================================
        TRUNCATING & INSERTING INTO silver.transactions
        ===========================================================
        */
        PRINT '================================================================';
        PRINT 'BEGINNING TRUNCATING & INSERTING INTO silver.transactions';
        PRINT '================================================================';

        PRINT '----------------------------------------------------------------';
        SET @start_time = GETDATE();
        PRINT '>>> Truncating table silver.transactions';
        TRUNCATE TABLE silver.transactions;
        PRINT '>>> Inserting into silver.transactions';
        INSERT INTO silver.transactions(
            transaction_id,
            account_id,
            transaction_date,
            transaction_type,
            amount,
            balance_after,
            merchant_name,
            merchant_category,
            description,
            status
        )
        SELECT 
            transaction_id,
            account_id,
            transaction_date,
            CASE
                WHEN LOWER(TRIM(transaction_type)) = 'atm' THEN 'ATM Withdrawal'
                WHEN LOWER(TRIM(transaction_type)) = 'withdrawal' THEN 'Withdrawal'
                WHEN LOWER(TRIM(transaction_type)) = 'deposit' THEN 'Deposit'
                WHEN LOWER(TRIM(transaction_type)) = 'direct deposit' THEN 'Direct Deposit'
                WHEN LOWER(TRIM(transaction_type)) = 'transfer' THEN 'Transfer'
                WHEN LOWER(TRIM(transaction_type)) = 'pos' THEN 'POS Payment'
                WHEN LOWER(TRIM(transaction_type)) = 'online payment' THEN 'Online Payment'
                WHEN LOWER(TRIM(transaction_type)) = 'interest' THEN 'Interest'
                ELSE 'Other'
            END AS transaction_type,
            amount,
            balance_after,
            CASE WHEN TRIM(merchant_name) IS NULL THEN 'Unknown' ELSE TRIM(merchant_name) END AS merchant_name,
            CASE WHEN TRIM(merchant_category) IS NULL THEN 'Unknown' ELSE TRIM(merchant_category) END AS merchant_category,
            TRIM(description) AS description,
            CASE 
                WHEN LOWER(TRIM(status)) = 'completed' THEN 'Completed'
                WHEN LOWER(TRIM(status)) = 'failed' THEN 'Failed'
                ELSE 'Unknown'
            END AS status
        FROM bronze.transactions;
        SET @end_time = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '----------------------------------------------------------------';


        COMMIT TRANSACTION;
        SET @batch_end_time = GETDATE();

        PRINT '================================================================================================================';
        PRINT 'LOADING THE SILVER LAYER IS COMPLETE';
        PRINT '>>> Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
        PRINT '================================================================================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================================================================================';
        PRINT 'ERROR OCCURED DURING - LOADING THE SILVER LAYER';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================================================================================';
    END CATCH
END
