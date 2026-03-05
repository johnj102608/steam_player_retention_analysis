/* ============================================================
   04_create_marts.sql
   Steam Market Analytics Warehouse — Marts for Tableau

   ============================================================ */

SET NOCOUNT ON;
GO

PRINT 'Step04: Creating mart views...';
GO

/* ============================================================
   6-month persistence proxy
   ratio = avg_players_month_index_1 / avg_players_month_index_7
   ============================================================ */
CREATE OR ALTER VIEW mart.v_app_persistence_6m AS
WITH m AS (
    SELECT
        a.app_id,
        a.app_sk,
        a.price_tier,
        a.is_free,
        d.month_index,
        f.avg_players
    FROM fact.monthly_players f
    JOIN dim.app  a ON a.app_sk  = f.app_sk
    JOIN dim.date d ON d.date_sk = f.date_sk
    WHERE d.month_index IN (1, 7)
),
p AS (
    SELECT
        app_id,
        app_sk,
        price_tier,
        is_free,
        MAX(CASE WHEN month_index = 1 THEN avg_players END) AS avg_players_recent,
        MAX(CASE WHEN month_index = 7 THEN avg_players END) AS avg_players_6mo_ago
    FROM m
    GROUP BY app_id, app_sk, price_tier, is_free
)
SELECT
    app_id,
    app_sk,
    price_tier,
    is_free,
    avg_players_recent,
    avg_players_6mo_ago,
    CASE
        WHEN avg_players_6mo_ago IS NULL OR avg_players_6mo_ago = 0 THEN NULL
        ELSE CAST(avg_players_recent AS float) / NULLIF(CAST(avg_players_6mo_ago AS float), 0.0)
    END AS persistence_6m_ratio
FROM p;
GO

/* ============================================================
   Genre retention (6-month proxy)
   bridge.app_genre + dim.genre
   ============================================================ */
CREATE OR ALTER VIEW mart.v_genre_retention_6m AS
WITH base AS (
    SELECT
        app_sk,
        avg_players_recent,
        avg_players_6mo_ago,
        persistence_6m_ratio
    FROM mart.v_app_persistence_6m
    WHERE avg_players_6mo_ago IS NOT NULL
      AND avg_players_6mo_ago > 0
      AND persistence_6m_ratio IS NOT NULL
)
SELECT
    g.genre_name,
    COUNT(DISTINCT b.app_sk) AS game_count,
    AVG(CAST(b.persistence_6m_ratio AS float)) AS avg_retention_ratio,
    AVG(CAST(b.avg_players_recent AS float)) AS avg_players_recent,
    AVG(CAST(b.avg_players_6mo_ago AS float)) AS avg_players_6mo_ago
FROM base b
JOIN bridge.app_genre ag ON ag.app_sk = b.app_sk
JOIN dim.genre g ON g.genre_sk = ag.genre_sk
GROUP BY g.genre_name;
GO

/* ============================================================
   Category retention (6-month proxy)
   bridge.app_category + dim.category
   ============================================================ */
CREATE OR ALTER VIEW mart.v_category_retention_6m AS
WITH base AS (
    SELECT
        app_sk,
        avg_players_recent,
        avg_players_6mo_ago,
        persistence_6m_ratio
    FROM mart.v_app_persistence_6m
    WHERE avg_players_6mo_ago IS NOT NULL
      AND avg_players_6mo_ago > 0
      AND persistence_6m_ratio IS NOT NULL
)
SELECT
    c.category_name,
    COUNT(DISTINCT b.app_sk) AS game_count,
    AVG(CAST(b.persistence_6m_ratio AS float)) AS avg_retention_ratio,
    AVG(CAST(b.avg_players_recent AS float)) AS avg_players_recent,
    AVG(CAST(b.avg_players_6mo_ago AS float)) AS avg_players_6mo_ago
FROM base b
JOIN bridge.app_category ac ON ac.app_sk = b.app_sk
JOIN dim.category c ON c.category_sk = ac.category_sk
GROUP BY c.category_name;
GO

/* ============================================================
   Price tier retention (6-month proxy)
   ============================================================ */
CREATE OR ALTER VIEW mart.v_price_tier_retention_6m AS
WITH base AS (
    SELECT
        price_tier,
        is_free,
        app_sk,
        avg_players_recent,
        avg_players_6mo_ago,
        persistence_6m_ratio
    FROM mart.v_app_persistence_6m
    WHERE avg_players_6mo_ago IS NOT NULL
      AND avg_players_6mo_ago > 0
      AND persistence_6m_ratio IS NOT NULL
)
SELECT
    price_tier,
    is_free,
    COUNT(DISTINCT app_sk) AS game_count,
    AVG(CAST(persistence_6m_ratio AS float)) AS avg_retention_ratio,
    AVG(CAST(avg_players_recent AS float)) AS avg_players_recent,
    AVG(CAST(avg_players_6mo_ago AS float)) AS avg_players_6mo_ago
FROM base
GROUP BY price_tier, is_free;
GO

/* ============================================================
   Player decay curve by month_index
   ============================================================ */
CREATE OR ALTER VIEW mart.v_player_decay_curve AS
SELECT
    d.month_index,
    d.month_label,
    COUNT(*) AS row_count,
    AVG(CAST(f.avg_players AS float)) AS avg_avg_players,
    AVG(CAST(f.peak_players AS float)) AS avg_peak_players
FROM fact.monthly_players f
JOIN dim.date d ON d.date_sk = f.date_sk
GROUP BY d.month_index, d.month_label;
GO

/* ============================================================
   Top growth/decay games (ranked by absolute log-change)
   ============================================================ */
CREATE OR ALTER VIEW mart.v_top_growth_decay_6m AS
SELECT TOP (2000)
    p.app_id,
    p.price_tier,
    p.is_free,
    p.avg_players_recent,
    p.avg_players_6mo_ago,
    p.persistence_6m_ratio,
    CASE WHEN p.persistence_6m_ratio >= 1.0 THEN 'Growth' ELSE 'Decay' END AS trend_bucket,
    ABS(LOG(NULLIF(CAST(p.persistence_6m_ratio AS float), 0.0))) AS abs_log_change
FROM mart.v_app_persistence_6m p
WHERE p.avg_players_6mo_ago IS NOT NULL
  AND p.avg_players_6mo_ago > 0
  AND p.persistence_6m_ratio IS NOT NULL
ORDER BY ABS(LOG(NULLIF(CAST(p.persistence_6m_ratio AS float), 0.0))) DESC;
GO

/* ============================================================
   04.7 — Checking
   ============================================================ */
PRINT 'Step04: QA counts';
SELECT COUNT(*) AS fact_monthly_players_rows FROM fact.monthly_players;
SELECT COUNT(*) AS dim_app_rows FROM dim.app;
SELECT COUNT(*) AS dim_date_rows FROM dim.date;
SELECT COUNT(*) AS bridge_app_genre_rows FROM bridge.app_genre;
SELECT COUNT(*) AS bridge_app_category_rows FROM bridge.app_category;

SELECT COUNT(*) AS mart_app_persistence_rows FROM mart.v_app_persistence_6m;
SELECT COUNT(*) AS mart_genre_retention_rows FROM mart.v_genre_retention_6m;
SELECT COUNT(*) AS mart_category_retention_rows FROM mart.v_category_retention_6m;
SELECT COUNT(*) AS mart_price_tier_retention_rows FROM mart.v_price_tier_retention_6m;
SELECT COUNT(*) AS mart_decay_curve_rows FROM mart.v_player_decay_curve;
SELECT COUNT(*) AS mart_top_growth_decay_rows FROM mart.v_top_growth_decay_6m;
GO

PRINT 'Step04: Done.';
GO

SELECT * FROM dim.category