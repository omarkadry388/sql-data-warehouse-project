*/

    Purpose  : Loads raw data from flat CSV files into the bronze schema tables 
               (staging area for raw ingestion).

    Details:
      1. Truncates target bronze tables before loading new data.
      2. Uses BULK INSERT to import CSV files from the local file system:
            - crm_cust_info
            - crm_sales_details
            - crm_prd_info
            - erp_CUST_AZ12
            - erp_LOC_A101
            - erp_PX_CAT_GV12
      3. Logs the load duration for each table using PRINT and DATEDIFF.
      4. Provides simple performance monitoring (per-table elapsed seconds).
    
    Notes:
      - Make sure CSV files are not open in Excel or another program 
        (to avoid error code 32).
      - SQL Server service account must have READ permissions on the file paths.
      - Intended as a simple, repeatable ingestion job (can be scheduled via SQL Agent).
    
    Usage:
        EXEC bronze.load_bronze;
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time datetime,@batch_end_time datetime;
	set @batch_start_time=getdate();
    -------------------------------------------
    -- Load crm_cust_info
    -------------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.crm_cust_info;

    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\omark\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> crm_cust_info load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    -------------------------------------------
    -- Load crm_sales_details
    -------------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.crm_sales_details;

    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\omark\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> crm_sales_details load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    -------------------------------------------
    -- Load crm_prd_info
    -------------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.crm_prd_info;

    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\omark\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> crm_prd_info load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    -------------------------------------------
    -- Load erp_CUST_AZ12
    -------------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.erp_CUST_AZ12;

    BULK INSERT bronze.erp_CUST_AZ12
    FROM 'C:\Users\omark\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> erp_CUST_AZ12 load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    -------------------------------------------
    -- Load erp_LOC_A101
    -------------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.erp_LOC_A101;

    BULK INSERT bronze.erp_LOC_A101
    FROM 'C:\Users\omark\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> erp_LOC_A101 load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    -------------------------------------------
    -- Load erp_PX_CAT_GV12
    -------------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.erp_PX_CAT_GV12;

    BULK INSERT bronze.erp_PX_CAT_GV12
    FROM 'C:\Users\omark\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> erp_PX_CAT_GV12 load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	set @batch_end_time=getdate()
    PRINT '>> whole load duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
END
exec bronze.load_bronze
