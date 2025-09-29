/**********************************************************************************************
    File: ddl_silver.sql
    Database: FinBankWarehouse
    Layer: Silver (Cleansed / Standardized Layer)

    Purpose:
        This script creates the Silver layer tables, which store standardized and cleansed 
        data based on the raw Bronze layer ingestion. Each table includes a `dwh_create_date`
        timestamp column to track load time for auditability.

        Existing tables are dropped before creation to allow re-runs during development.

    Author: SINGOEI RODGERS
    Created On: 29/09/2025
**********************************************************************************************/

USE FinBankWarehouse;
GO

/*============================================================
  silver.accounts - Cleansed & Standardized accounts records
============================================================*/
IF OBJECT_ID ('silver.accounts', 'U') IS NOT NULL
    DROP TABLE silver.accounts;
CREATE TABLE silver.accounts(
    account_id NVARCHAR(50),
    customer_id NVARCHAR(50),
    account_type NVARCHAR(50),
    account_number NVARCHAR(50),
    current_balance FLOAT,
    open_date DATE,
    interest_rate FLOAT,
    status NVARCHAR(MAX),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/*============================================================
  silver.branches - Cleansed & Standardized branch information
============================================================*/
IF OBJECT_ID ('silver.branches', 'U') IS NOT NULL
    DROP TABLE silver.branches;
CREATE TABLE silver.branches(
    branch_id NVARCHAR(50),
    branch_name NVARCHAR(50),
    city NVARCHAR(50),
    state NVARCHAR(50),
    zip_code NVARCHAR(50),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    opening_date DATE,
    total_deposits INT,
    employee_count INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/*============================================================
  silver.credit_cards - Cleansed & Standardized credit card accounts details
============================================================*/
IF OBJECT_ID ('silver.credit_cards', 'U') IS NOT NULL
    DROP TABLE silver.credit_cards;
CREATE TABLE silver.credit_cards(
    card_id NVARCHAR(50),
    customer_id NVARCHAR(50),
    card_number NVARCHAR(50),
    expiry_date DATE,
    credit_limit INT,
    current_balance INT,
    available_credit INT,
    issue_date DATE,
    card_type NVARCHAR(50),
    status NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/*============================================================
  silver.customers - Cleansed & Standardized customer information
============================================================*/
IF OBJECT_ID ('silver.customers', 'U') IS NOT NULL
    DROP TABLE silver.customers;
CREATE TABLE silver.customers(
    customer_id NVARCHAR(50),
    first_name NVARCHAR(50),
    last_name NVARCHAR(50),
    email NVARCHAR(50),
    phone NVARCHAR(50),
    address NVARCHAR(50),
    city NVARCHAR(50),
    state NVARCHAR(50),
    zip_code NVARCHAR(50),
    date_of_birth DATE,
    ssn INT,
    customer_since DATE,
    credit_score INT,
    annual_income INT,
    employment_status NVARCHAR(50),
    branch_id NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/*============================================================
  silver.loans - Cleansed & Standardized loan details
============================================================*/
IF OBJECT_ID ('silver.loans', 'U') IS NOT NULL
    DROP TABLE silver.loans;
CREATE TABLE silver.loans(
    loan_id NVARCHAR(50),
    customer_id NVARCHAR(50),
    loan_type NVARCHAR(50),
    loan_amount INT,
    interest_rate FLOAT,
    term_months INT,
    start_date DATE,
    monthly_payment FLOAT,
    remaining_balance FLOAT,
    status NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/*============================================================
  silver.transactions - Cleansed & Standardized customer transactions
============================================================*/
IF OBJECT_ID ('silver.transactions', 'U') IS NOT NULL
    DROP TABLE silver.transactions;
CREATE TABLE silver.transactions(
    transaction_id NVARCHAR(50),
    account_id NVARCHAR(50),
    transaction_date DATETIME2,
    transaction_type NVARCHAR(50), 
    amount FLOAT,
    balance_after FLOAT,
    merchant_name NVARCHAR(50),
    merchant_category NVARCHAR(50),
    description NVARCHAR(100),
    status NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
