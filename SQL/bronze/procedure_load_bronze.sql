/**********************************************************************************************
    File: procedure_load_bronze.sql
    Database: FinBankWarehouse
    Layer: Bronze (Raw Ingest Layer)

    Purpose:
        This stored procedure loads raw CSV data into the Bronze layer tables.
        Each table is truncated before loading to allow full reloads.
        BULK INSERT is used for fast ingestion without transformations.

    Execution:
        EXEC bronze.load_bronze;

    Author: SINGOEI RODGERS
    Created On: 27/09/2025
**********************************************************************************************/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    -- Declaring timing variables for performance tracking
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		BEGIN TRANSACTION;
		PRINT '=================================================';
        PRINT 'LOADING THE BRONZE LAYER';
		PRINT '=================================================';

        /**********************************************
            LOAD TABLE: bronze.accounts
        **********************************************/
		PRINT '-------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.accounts';
		TRUNCATE TABLE bronze.accounts;

		PRINT '>>> Inserting Into: bronze.accounts';
		BULK INSERT bronze.accounts
		FROM 'C:\Users\user 1\RKS\finbank\finbank_data\accounts.csv'
		WITH(
			FIRSTROW = 2,             -- Skipping the header row(i.e the column names)
			FIELDTERMINATOR = ',',    -- CSV delimiter
			TABLOCK                   -- Optimized bulk lock
		);
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------------------';

        /**********************************************
            LOAD TABLE: bronze.branches
        **********************************************/
		PRINT '-------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.branches';
		TRUNCATE TABLE bronze.branches;

		PRINT '>>> Inserting Into: bronze.branches';
		BULK INSERT bronze.branches
		FROM 'C:\Users\user 1\RKS\finbank\finbank_data\branches.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------------------';

        /**********************************************
            LOAD TABLE: bronze.credit_cards
        **********************************************/
		PRINT '-------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.credit_cards';
		TRUNCATE TABLE bronze.credit_cards;

		PRINT '>>> Inserting Into: bronze.credit_cards';
		BULK INSERT bronze.credit_cards
		FROM 'C:\Users\user 1\RKS\finbank\finbank_data\credit_cards.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------------------';

        /**********************************************
            LOAD TABLE: bronze.customers
        **********************************************/
		PRINT '-------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.customers';
		TRUNCATE TABLE bronze.customers;

		PRINT '>>> Inserting Into: bronze.customers';
		BULK INSERT bronze.customers
		FROM 'C:\Users\user 1\RKS\finbank\finbank_data\customers.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------------------';

        /**********************************************
            LOAD TABLE: bronze.loans
        **********************************************/
		PRINT '-------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.loans';
		TRUNCATE TABLE bronze.loans;

		PRINT '>>> Inserting Into: bronze.loans';
		BULK INSERT bronze.loans
		FROM 'C:\Users\user 1\RKS\finbank\finbank_data\loans.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------------------';

        /**********************************************
            LOAD TABLE: bronze.transactions
        **********************************************/
		PRINT '-------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.transactions';
		TRUNCATE TABLE bronze.transactions;

		PRINT '>>> Inserting Into: bronze.transactions';
		BULK INSERT bronze.transactions
		FROM 'C:\Users\user 1\RKS\finbank\finbank_data\transactions.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------------------';
		COMMIT TRANSACTION;
		SET @batch_end_time = GETDATE();
		PRINT '=================================================';
		PRINT 'LOADING THE BRONZE LAYER IS COMPLETE';
		PRINT '>>> Total Load Duration: ' + CAST (DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '=================================================';

	END TRY
	BEGIN CATCH
		PRINT '=================================================';
		PRINT 'ERROR OCCURRED DURING - LOADING THE BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=================================================';
	END CATCH
END;
