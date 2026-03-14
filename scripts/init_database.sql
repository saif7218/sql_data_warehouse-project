-- Create main database for the data warehouse
CREATE DATABASE DataWarehouse;
GO

-- Switch to the newly created database context
USE DataWarehouse;
GO

-- Bronze schema: Raw data ingestion layer (unchanged source data)
CREATE SCHEMA bronze;
GO

-- Silver schema: Cleaned and transformed data layer
CREATE SCHEMA silver;
GO

-- Gold schema: Business-ready aggregated data layer for analytics
CREATE SCHEMA gold;
GO
