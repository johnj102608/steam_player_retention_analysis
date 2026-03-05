/* 01_create_schemas_and_tables.sql
   Star schema
   Strategy:
     - Drop views first
     - Drop facts, then bridges, then dims, then staging
     - Recreate tables cleanly
*/

USE SteamForecast;
GO

/* -------------------------
   Schemas
------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')    EXEC('CREATE SCHEMA stg');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim')    EXEC('CREATE SCHEMA dim');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bridge') EXEC('CREATE SCHEMA bridge');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fact')   EXEC('CREATE SCHEMA fact');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart')   EXEC('CREATE SCHEMA mart');
GO

/* -------------------------
   Drop views (mart)
------------------------- */
IF OBJECT_ID('mart.v_app_persistence_6m', 'V') IS NOT NULL DROP VIEW mart.v_app_persistence_6m;
GO

/* -------------------------
   Drop facts (depend on dims)
------------------------- */
IF OBJECT_ID('fact.monthly_players', 'U') IS NOT NULL DROP TABLE fact.monthly_players;
GO

/* -------------------------
   Drop bridges (depend on dims)
------------------------- */
IF OBJECT_ID('bridge.app_genre', 'U') IS NOT NULL DROP TABLE bridge.app_genre;
IF OBJECT_ID('bridge.app_category', 'U') IS NOT NULL DROP TABLE bridge.app_category;
GO

/* -------------------------
   Drop dims (parents)
------------------------- */
IF OBJECT_ID('dim.genre', 'U') IS NOT NULL DROP TABLE dim.genre;
IF OBJECT_ID('dim.category', 'U') IS NOT NULL DROP TABLE dim.category;
IF OBJECT_ID('dim.[date]', 'U') IS NOT NULL DROP TABLE dim.[date];
IF OBJECT_ID('dim.app', 'U') IS NOT NULL DROP TABLE dim.app;
GO

/* -------------------------
   Drop staging last
------------------------- */
IF OBJECT_ID('stg.raw_steamcharts_monthly_long', 'U') IS NOT NULL DROP TABLE stg.raw_steamcharts_monthly_long;
IF OBJECT_ID('stg.raw_store_meta', 'U') IS NOT NULL DROP TABLE stg.raw_store_meta;
IF OBJECT_ID('stg.raw_store_meta_landing', 'U') IS NOT NULL DROP TABLE stg.raw_store_meta_landing;
GO

/* ============================================================
   Recreate STAGING tables
============================================================ */

CREATE TABLE stg.raw_steamcharts_monthly_long (
    app_id        INT           NOT NULL,
    month_index   INT           NOT NULL,
    month_label   NVARCHAR(50)  NULL,
    avg_players   FLOAT         NULL,
    peak_players  FLOAT         NULL,
    scraped_utc   NVARCHAR(40)  NULL,
    url           NVARCHAR(300) NULL
);
GO

CREATE TABLE stg.raw_store_meta_landing (
    app_id           NVARCHAR(50)  NULL,
    is_free          NVARCHAR(10)  NULL,      -- TRUE/FALSE
    price_usd        NVARCHAR(50)  NULL,
    release_date_str NVARCHAR(200) NULL,
    genres_csv       NVARCHAR(MAX) NULL,
    categories_csv   NVARCHAR(MAX) NULL,
    scraped_utc      NVARCHAR(50)  NULL
);
GO

CREATE TABLE stg.raw_store_meta (
    app_id           INT           NOT NULL,
    is_free          INT           NULL,      -- 0/1 normalized
    price_usd        FLOAT         NULL,
    release_date_str NVARCHAR(200) NULL,
    genres_csv       NVARCHAR(MAX) NULL,
    categories_csv   NVARCHAR(MAX) NULL,
    scraped_utc      NVARCHAR(50)  NULL
);
GO

/* ============================================================
   Recreate STAR DIMENSIONS
============================================================ */

CREATE TABLE dim.app (
    app_sk           INT IDENTITY(1,1) PRIMARY KEY,
    app_id           INT           NOT NULL UNIQUE,
    is_free          BIT           NULL,
    price_usd        FLOAT         NULL,
    price_tier       NVARCHAR(20)  NULL,
    release_date     DATE          NULL,
    release_date_str NVARCHAR(200) NULL
);
GO

CREATE TABLE dim.[date] (
    date_sk          INT IDENTITY(1,1) PRIMARY KEY,
    month_start_date DATE          NULL,      -- best effort parse
    year_num         INT           NULL,
    month_num        INT           NULL,
    month_label      NVARCHAR(50)  NULL,
    month_index      INT           NULL,      -- 1..12 (relative)
    CONSTRAINT UQ_dim_date UNIQUE (month_label, month_index)
);
GO

CREATE TABLE dim.genre (
    genre_sk   INT IDENTITY(1,1) PRIMARY KEY,
    genre_name NVARCHAR(200) NOT NULL,
    genre_norm NVARCHAR(200) NOT NULL UNIQUE
);
GO

CREATE TABLE dim.category (
    category_sk   INT IDENTITY(1,1) PRIMARY KEY,
    category_name NVARCHAR(200) NOT NULL,
    category_norm NVARCHAR(200) NOT NULL UNIQUE
);
GO

/* ============================================================
   Recreate BRIDGES
============================================================ */

CREATE TABLE bridge.app_genre (
    app_sk   INT NOT NULL,
    genre_sk INT NOT NULL,
    CONSTRAINT PK_bridge_app_genre PRIMARY KEY (app_sk, genre_sk),
    CONSTRAINT FK_bridge_app_genre_app   FOREIGN KEY (app_sk) REFERENCES dim.app(app_sk),
    CONSTRAINT FK_bridge_app_genre_genre FOREIGN KEY (genre_sk) REFERENCES dim.genre(genre_sk)
);
GO

CREATE TABLE bridge.app_category (
    app_sk      INT NOT NULL,
    category_sk INT NOT NULL,
    CONSTRAINT PK_bridge_app_category PRIMARY KEY (app_sk, category_sk),
    CONSTRAINT FK_bridge_app_category_app      FOREIGN KEY (app_sk) REFERENCES dim.app(app_sk),
    CONSTRAINT FK_bridge_app_category_category FOREIGN KEY (category_sk) REFERENCES dim.category(category_sk)
);
GO

/* ============================================================
   Recreate STAR FACT
============================================================ */

CREATE TABLE fact.monthly_players (
    app_sk       INT NOT NULL,
    date_sk      INT NOT NULL,
    avg_players  FLOAT NULL,
    peak_players FLOAT NULL,
    scraped_utc  NVARCHAR(40) NULL,
    CONSTRAINT PK_fact_monthly_players PRIMARY KEY (app_sk, date_sk),
    CONSTRAINT FK_fact_monthly_players_app  FOREIGN KEY (app_sk) REFERENCES dim.app(app_sk),
    CONSTRAINT FK_fact_monthly_players_date FOREIGN KEY (date_sk) REFERENCES dim.[date](date_sk)
);
GO

CREATE INDEX IX_fact_monthly_players_app  ON fact.monthly_players(app_sk);
CREATE INDEX IX_fact_monthly_players_date ON fact.monthly_players(date_sk);
GO