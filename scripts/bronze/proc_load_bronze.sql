
CREATE OR ALTER PROCEDURE bronze.LOAD_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    
    BEGIN TRY
        -- =============================================
        -- Print start of Bronze Layer Loading
        -- =============================================
        PRINT '============================================';
        PRINT 'Loading Bronze Layer';
        PRINT '============================================';

        -- =============================================
        -- Setup sales_details table with NVARCHAR columns
        -- =============================================
        PRINT '--------------------------------------------';
        PRINT 'Setting up sales_details table';
        PRINT '--------------------------------------------';
        
        IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
            DROP TABLE bronze.crm_sales_details;
        
        CREATE TABLE bronze.crm_sales_details (
            sls_ord_num   NVARCHAR(50),
            sls_prd_key   NVARCHAR(50),
            sls_cust_id   NVARCHAR(50),
            sls_order_dt  NVARCHAR(50),
            sls_ship_dt   NVARCHAR(50),
            sls_due_dt    NVARCHAR(50),
            sls_sales     NVARCHAR(50),
            sls_quantity  NVARCHAR(50),
            sls_price     NVARCHAR(50)
        );
        PRINT '✓ sales_details table recreated successfully';

        -- =============================================
        -- Load CRM Tables
        -- =============================================
        PRINT '--------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '--------------------------------------------';
        
        -- Load CRM Customer Info
        SET @start_time = GETDATE();
        PRINT 'Loading crm_cust_info...';
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info 
        FROM 'C:\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            firstrow         = 2,
            fieldterminator  = ',',
            tablock
        );
        SET @end_time = GETDATE();
        PRINT '✓ crm_cust_info loaded successfully (Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's)';

        -- Load CRM Product Info
        SET @start_time = GETDATE();
        PRINT 'Loading crm_prd_info...';
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info 
        FROM 'C:\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            firstrow         = 2,
            fieldterminator  = ',',
            tablock
        );
        SET @end_time = GETDATE();
        PRINT '✓ crm_prd_info loaded successfully (Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's)';

        -- Load CRM Sales Details
        SET @start_time = GETDATE();
        PRINT 'Loading crm_sales_details...';
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details 
        FROM 'C:\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            firstrow         = 2,
            fieldterminator  = ',',
            tablock
        );
        SET @end_time = GETDATE();
        PRINT '✓ crm_sales_details loaded successfully (Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's)';

        -- =============================================
        -- Load ERP Tables
        -- =============================================
        PRINT '--------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '--------------------------------------------';

        -- Load ERP Location A101
        SET @start_time = GETDATE();
        PRINT 'Loading erp_loc_a101...';
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101 
        FROM 'C:\sql\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            firstrow         = 2,
            fieldterminator  = ',',
            tablock
        );
        SET @end_time = GETDATE();
        PRINT '✓ erp_loc_a101 loaded successfully (Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's)';

        -- Load ERP Customer AZ12
        SET @start_time = GETDATE();
        PRINT 'Loading erp_cust_az12...';
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12 
        FROM 'C:\sql\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            firstrow         = 2,
            fieldterminator  = ',',
            tablock
        );
        SET @end_time = GETDATE();
        PRINT '✓ erp_cust_az12 loaded successfully (Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's)';

        -- Load ERP PX Cat G1V2
        SET @start_time = GETDATE();
        PRINT 'Loading erp_px_cat_g1v2...';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2 
        FROM 'C:\sql\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            firstrow         = 2,
            fieldterminator  = ',',
            tablock
        );
        SET @end_time = GETDATE();
        PRINT '✓ erp_px_cat_g1v2 loaded successfully (Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's)';

        -- =============================================
        -- Completion Message
        -- =============================================
        PRINT '--------------------------------------------';
        PRINT 'Bronze Layer Loading Completed Successfully';
        PRINT '============================================';
        
    END TRY
    BEGIN CATCH
        PRINT '============================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LAYER LOADING';
        PRINT '============================================';
        PRINT 'Error Number   : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message  : ' + ERROR_MESSAGE();
        PRINT 'Error State    : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Error Severity : ' + CAST(ERROR_SEVERITY() AS NVARCHAR);
        PRINT 'Error Line     : ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT '============================================';
    END CATCH
END
