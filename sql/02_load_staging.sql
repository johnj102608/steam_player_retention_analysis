USE SteamForecast;
GO

TRUNCATE TABLE stg.raw_steamcharts_monthly_long;
TRUNCATE TABLE stg.raw_store_meta_landing;
TRUNCATE TABLE stg.raw_store_meta;
GO

/* -------------------------
   Load SteamCharts
------------------------- */

BULK INSERT stg.raw_steamcharts_monthly_long
FROM 'C:\SQLdata\steam_forecast\steamcharts_monthly_long.csv'

WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
);
GO

/* -------------------------
   Load Store Meta (landing)
------------------------- */

BULK INSERT stg.raw_store_meta_landing
FROM 'C:\SQLdata\steam_forecast\store_meta_clean.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
);
GO
