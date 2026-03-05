/* =========================================================
   00_create_database.sql
   Steam Forecast Warehouse
   Fully rerunnable — drops DB if exists
   ========================================================= */

USE master;
GO

/* ========================================
   If database exists, force drop safely
   ======================================== */

IF DB_ID('SteamForecast') IS NOT NULL
BEGIN
    ALTER DATABASE SteamForecast SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE SteamForecast;
END
GO


/* ========================================
   Create fresh database
   ======================================== */

CREATE DATABASE SteamForecast;
GO

ALTER DATABASE SteamForecast SET RECOVERY SIMPLE;
GO

ALTER DATABASE SteamForecast SET ALLOW_SNAPSHOT_ISOLATION ON;
ALTER DATABASE SteamForecast SET READ_COMMITTED_SNAPSHOT ON;
GO


USE SteamForecast;
GO

PRINT 'SteamForecast database created successfully.';
GO