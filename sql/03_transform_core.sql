/* 03_transform_core.sql
   Build star schema from staging tables
*/

USE SteamForecast;
GO

/* -------------------------
   A) Convert landing -> typed store meta
------------------------- */
TRUNCATE TABLE stg.raw_store_meta;
GO

INSERT INTO stg.raw_store_meta (
    app_id, is_free, price_usd, release_date_str, genres_csv, categories_csv, scraped_utc
)
SELECT
    TRY_CONVERT(INT, app_id) AS app_id,
    CASE
        WHEN UPPER(LTRIM(RTRIM(is_free))) IN ('TRUE','1','YES','Y') THEN 1
        WHEN UPPER(LTRIM(RTRIM(is_free))) IN ('FALSE','0','NO','N') THEN 0
        ELSE NULL
    END AS is_free,
    TRY_CONVERT(FLOAT, price_usd) AS price_usd,
    NULLIF(release_date_str,'') AS release_date_str,
    NULLIF(genres_csv,'') AS genres_csv,
    NULLIF(categories_csv,'') AS categories_csv,
    NULLIF(scraped_utc,'') AS scraped_utc
FROM stg.raw_store_meta_landing
WHERE TRY_CONVERT(INT, app_id) IS NOT NULL;
GO


/* -------------------------
   B) dim.app upsert
------------------------- */
MERGE dim.app AS tgt
USING (
    SELECT
        app_id,
        CAST(is_free AS BIT) AS is_free,
        price_usd,
        release_date_str
    FROM stg.raw_store_meta
) AS src
ON tgt.app_id = src.app_id
WHEN MATCHED THEN
  UPDATE SET
    tgt.is_free = src.is_free,
    tgt.price_usd = src.price_usd,
    tgt.release_date_str = src.release_date_str
WHEN NOT MATCHED THEN
  INSERT (app_id, is_free, price_usd, release_date_str)
  VALUES (src.app_id, src.is_free, src.price_usd, src.release_date_str);
GO

-- price tier (simple, tweak later)
UPDATE dim.app
SET price_tier =
    CASE
      WHEN is_free = 1 OR price_usd IS NULL OR price_usd = 0 THEN 'Free'
      WHEN price_usd < 10 THEN 'Under_10'
      WHEN price_usd < 30 THEN '10_29'
      WHEN price_usd < 60 THEN '30_59'
      ELSE '60_plus'
    END;
GO

-- parse to date
UPDATE dim.app
SET release_date = TRY_CONVERT(date, release_date_str)
WHERE release_date IS NULL AND release_date_str IS NOT NULL;
GO

/* -------------------------
   C) dim.date (month-grain)
   We keep both month_label + month_index (relative) since parsing labels may fail sometimes.
------------------------- */
MERGE dim.[date] AS tgt
USING (
    SELECT DISTINCT
        NULLIF(month_label,'') AS month_label,
        month_index
    FROM stg.raw_steamcharts_monthly_long
) AS src
ON tgt.month_label = src.month_label AND tgt.month_index = src.month_index
WHEN NOT MATCHED THEN
  INSERT (month_label, month_index)
  VALUES (src.month_label, src.month_index);
GO

-- Try to parse month_start_date from month_label like 'February 2024'
-- If it fails, month_start_date remains NULL.
UPDATE dim.[date]
SET month_start_date = TRY_CONVERT(date, '1 ' + month_label)
WHERE month_start_date IS NULL AND month_label IS NOT NULL;
GO

UPDATE dim.[date]
SET
  year_num = YEAR(month_start_date),
  month_num = MONTH(month_start_date)
WHERE month_start_date IS NOT NULL;
GO

/* -------------------------
   D) Genres + Categories dims and bridges
------------------------- */

-- Normalize helper (inline)
;WITH app_map AS (
    SELECT a.app_sk, s.app_id, s.genres_csv, s.categories_csv
    FROM stg.raw_store_meta s
    JOIN dim.app a ON a.app_id = s.app_id
),
genre_items AS (
    SELECT
        app_sk,
        LTRIM(RTRIM(value)) AS genre_name,
        LOWER(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(value)), '-', ' '), '_', ' '), '  ', ' ')) AS genre_norm
    FROM app_map
    CROSS APPLY string_split(ISNULL(genres_csv,''), '|')
    WHERE LTRIM(RTRIM(value)) <> ''
),
cat_items AS (
    SELECT
        app_sk,
        LTRIM(RTRIM(value)) AS category_name,
        LOWER(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(value)), '-', ' '), '_', ' '), '  ', ' ')) AS category_norm
    FROM app_map
    CROSS APPLY string_split(ISNULL(categories_csv,''), '|')
    WHERE LTRIM(RTRIM(value)) <> ''
)
MERGE dim.genre AS tgt
USING (SELECT DISTINCT genre_name, genre_norm FROM genre_items) AS src
ON tgt.genre_norm = src.genre_norm
WHEN NOT MATCHED THEN
  INSERT (genre_name, genre_norm)
  VALUES (src.genre_name, src.genre_norm);
GO

SELECT * FROM dim.genre

-- Upsert dim.category
;WITH app_map AS (
    SELECT a.app_sk, s.app_id, s.categories_csv
    FROM stg.raw_store_meta s
    JOIN dim.app a ON a.app_id = s.app_id
),
cat_items AS (
    SELECT
        app_sk,
        LTRIM(RTRIM(value)) AS category_name,
        LOWER(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(value)), '-', ' '), '_', ' '), '  ', ' ')) AS category_norm
    FROM app_map
    CROSS APPLY string_split(ISNULL(categories_csv,''), '|')
    WHERE LTRIM(RTRIM(value)) <> ''
)
MERGE dim.category AS tgt
USING (SELECT DISTINCT category_name, category_norm FROM cat_items) AS src
ON tgt.category_norm = src.category_norm
WHEN NOT MATCHED THEN
  INSERT (category_name, category_norm)
  VALUES (src.category_name, src.category_norm);
GO

-- Bridge app_genre (insert only)
;WITH app_map AS (
    SELECT a.app_sk, s.genres_csv
    FROM stg.raw_store_meta s
    JOIN dim.app a ON a.app_id = s.app_id
),
genre_items AS (
    SELECT
        app_sk,
        LOWER(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(value)), '-', ' '), '_', ' '), '  ', ' ')) AS genre_norm
    FROM app_map
    CROSS APPLY string_split(ISNULL(genres_csv,''), '|')
    WHERE LTRIM(RTRIM(value)) <> ''
)
INSERT INTO bridge.app_genre (app_sk, genre_sk)
SELECT DISTINCT
    gi.app_sk,
    g.genre_sk
FROM genre_items gi
JOIN dim.genre g ON g.genre_norm = gi.genre_norm
WHERE NOT EXISTS (
    SELECT 1 FROM bridge.app_genre b
    WHERE b.app_sk = gi.app_sk AND b.genre_sk = g.genre_sk
);
GO

-- Bridge app_category (insert only)
;WITH app_map AS (
    SELECT a.app_sk, s.categories_csv
    FROM stg.raw_store_meta s
    JOIN dim.app a ON a.app_id = s.app_id
),
cat_items AS (
    SELECT
        app_sk,
        LOWER(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(value)), '-', ' '), '_', ' '), '  ', ' ')) AS category_norm
    FROM app_map
    CROSS APPLY string_split(ISNULL(categories_csv,''), '|')
    WHERE LTRIM(RTRIM(value)) <> ''
)
INSERT INTO bridge.app_category (app_sk, category_sk)
SELECT DISTINCT
    ci.app_sk,
    c.category_sk
FROM cat_items ci
JOIN dim.category c ON c.category_norm = ci.category_norm
WHERE NOT EXISTS (
    SELECT 1 FROM bridge.app_category b
    WHERE b.app_sk = ci.app_sk AND b.category_sk = c.category_sk
);
GO

/* -------------------------
   E) fact.monthly_players
------------------------- */

-- Delete existing matches then insert (simple & safe)
DELETE f
FROM fact.monthly_players f
JOIN dim.app a ON a.app_sk = f.app_sk
JOIN dim.[date] d ON d.date_sk = f.date_sk
JOIN stg.raw_steamcharts_monthly_long s
  ON s.app_id = a.app_id
 AND s.month_index = d.month_index
 AND s.month_label = d.month_label;
GO

INSERT INTO fact.monthly_players (app_sk, date_sk, avg_players, peak_players, scraped_utc)
SELECT
    a.app_sk,
    d.date_sk,
    s.avg_players,
    s.peak_players,
    s.scraped_utc
FROM stg.raw_steamcharts_monthly_long s
JOIN dim.app a
  ON a.app_id = s.app_id
JOIN dim.[date] d
  ON d.month_index = s.month_index
 AND d.month_label = s.month_label;
GO

