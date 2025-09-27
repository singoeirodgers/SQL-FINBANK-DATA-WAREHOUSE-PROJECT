/**********************************************************************************************
    File: ddl_bronze.sql
    Database: FinBankWarehouse
    Layer: Bronze (Raw Ingest Layer)

    Purpose:
        This script creates the raw data tables for the Bronze layer of the FinBank data warehouse.
        These tables store unprocessed, source-level data exactly as ingested.
        No transformations, cleansing, or standardization are applied at this stage.
        
        Each table is dropped before creation to allow for full reloads during development/testing.

    Author: SINGOEI RODGERS
    Created On: 27/09/2025
**********************************************************************************************/

/*============================================================
  BRONZE.ACCOUNTS - Raw bank account records
============================================================*/
IF OBJECT_ID ('bronze.accounts', 'U') IS NOT NULL
    DROP TABLE bronze.accounts;
CREATE TABLE bronze.accounts(
    account_id       NVARCHAR(50),
    customer_id      NVARCHAR(50),
    account_type     NVARCHAR(50),
    account_number   NVARCHAR(50),
    current_balance  FLOAT,
    open_date        DATE,
    interest_rate    FLOAT,
    status           NVARCHAR(50)
);
GO

/*============================================================
  BRONZE.BRANCHES - Raw branch information
============================================================*/
IF OBJECT_ID ('bronze.branches', 'U') IS NOT NULL
    DROP TABLE bronze.branches;
CREATE TABLE bronze.branches(
    branch_id       NVARCHAR(50),
    branch_name     NVARCHAR(50),
    city            NVARCHAR(50),      
    state           NVARCHAR(50),
    zip_code        NVARCHAR(50),
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    opening_date    DATE,
    total_deposits  INT,
    employee_count  INT
);
GO

/*============================================================
  BRONZE.CREDIT_CARDS - Raw credit card account records
============================================================*/
IF OBJECT_ID ('bronze.credit_cards', 'U') IS NOT NULL
    DROP TABLE bronze.credit_cards;
CREATE TABLE bronze.credit_cards(
    card_id         NVARCHAR(50),
    customer_id     NVARCHAR(50),
    card_number     NVARCHAR(50),
    expiry_date     DATE,
    credit_limit    INT,
    current_balance INT,
    available_credit INT,
    issue_date      DATE,
    card_type       NVARCHAR(50),
    status          NVARCHAR(50)
);
GO

/*============================================================
  BRONZE.CUSTOMERS - Raw customer master data
============================================================*/
IF OBJECT_ID ('bronze.customers', 'U') IS NOT NULL
    DROP TABLE bronze.customers;
CREATE TABLE bronze.customers(
    customer_id        NVARCHAR(50),
    first_name         NVARCHAR(50),
    last_name          NVARCHAR(50),
    email              NVARCHAR(50),
    phone              NVARCHAR(50),
    address            NVARCHAR(50),
    city               NVARCHAR(50),
    state              NVARCHAR(50),
    zip_code           NVARCHAR(50),
    date_of_birth      DATE,
    ssn                INT,
    customer_since     DATE,
    credit_score       INT,
    annual_income      INT,
    employment_status  NVARCHAR(50),
    branch_id          NVARCHAR(50)
);
GO

/*============================================================
  BRONZE.LOANS - Raw loan account records
============================================================*/
IF OBJECT_ID ('bronze.loans', 'U') IS NOT NULL
    DROP TABLE bronze.loans;
CREATE TABLE bronze.loans(
    loan_id            NVARCHAR(50),
    customer_id        NVARCHAR(50),
    loan_type          NVARCHAR(50),
    loan_amount        INT,
    interest_rate      FLOAT,
    term_months        INT,
    start_date         DATE,
    monthly_payment    FLOAT,
    remaining_balance  FLOAT,
    status             NVARCHAR(50)
);
GO

/*============================================================
  BRONZE.TRANSACTIONS - Raw financial transactions
============================================================*/
IF OBJECT_ID ('bronze.transactions', 'U') IS NOT NULL
    DROP TABLE bronze.transactions;
CREATE TABLE bronze.transactions(
    transaction_id     NVARCHAR(50),
    account_id         NVARCHAR(50),
    transaction_date   DATETIME,
    transaction_type   NVARCHAR(50),
    amount             FLOAT,
    balance_after      FLOAT,
    merchant_name      NVARCHAR(50),
    merchant_category  NVARCHAR(50),
    description        NVARCHAR(100),
    status             NVARCHAR(50)
);
GO
