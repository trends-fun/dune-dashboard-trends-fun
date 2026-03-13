-- part of a query repo
-- query name: Trends
-- query link: https://dune.com/queries/5555117
-- purpose: Calculate launch/migration totals and graduation rates (overall and last 7 days), plus weekly launch-migration trends for the last 3 months.


WITH summary_stats AS (
    SELECT
        COUNT(*) AS total_coins_launched,
        COUNT(CASE WHEN call_block_time >= date_add('day', -7, NOW()) THEN 1 END) AS total_coins_launched_last_7_days
    FROM meteora_solana.dynamic_bonding_curve_call_initialize_virtual_pool_with_token2022
    WHERE account_config in (
        '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
        '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
        '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
        '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
    )
),

migration_stats AS (
    SELECT
        COUNT(*) AS total_coins_migration,
        COUNT(CASE WHEN call_block_time >= date_add('day', -7, NOW()) THEN 1 END) AS total_coins_migration_last_7_days
    FROM meteora_solana.dynamic_bonding_curve_call_migration_damm_v2
    WHERE account_config in (
        '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
        '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
        '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
        '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
    )
),

weekly_launch_stats AS (
    SELECT
        date_trunc('week', call_block_time) AS week_start,
        COUNT(*) AS launch_count
    FROM meteora_solana.dynamic_bonding_curve_call_initialize_virtual_pool_with_token2022
    WHERE account_config in (
        '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
        '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
        '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
        '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
    )
        AND call_block_time >= date_add('month', -3, NOW()) 
    GROUP BY date_trunc('week', call_block_time)
),

weekly_migration_stats AS (
    SELECT
        date_trunc('week', call_block_time) AS week_start,
        COUNT(*) AS migration_count
    FROM meteora_solana.dynamic_bonding_curve_call_migration_damm_v2
    WHERE account_config in (
        '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
        '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
        '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
        '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
    )
        AND call_block_time >= date_add('month', -3, NOW()) 
    GROUP BY date_trunc('week', call_block_time)
),

weekly_combined AS (
    SELECT
        COALESCE(l.week_start, m.week_start) AS week_start,
        COALESCE(l.launch_count, 0) AS launch_count,
        COALESCE(m.migration_count, 0) AS migration_count
    FROM weekly_launch_stats l
    FULL OUTER JOIN weekly_migration_stats m
        ON l.week_start = m.week_start
)

SELECT
    'summary' AS data_type,
    NULL AS week_start,
    s.total_coins_launched,
    s.total_coins_launched_last_7_days,
    m.total_coins_migration,
    m.total_coins_migration_last_7_days,
    CASE 
        WHEN s.total_coins_launched > 0 
        THEN ROUND((m.total_coins_migration * 100.0 / s.total_coins_launched), 2)
        ELSE 0 
    END AS total_graduation_rate_percent,
    CASE 
        WHEN s.total_coins_launched_last_7_days > 0 
        THEN ROUND((m.total_coins_migration_last_7_days * 100.0 / s.total_coins_launched_last_7_days), 2)
        ELSE 0 
    END AS last_7_days_graduation_rate_percent,
    NULL AS launch_count,
    NULL AS migration_count,
    NULL AS weekly_graduation_rate_percent
FROM summary_stats s
CROSS JOIN migration_stats m

UNION ALL

SELECT
    'weekly' AS data_type,
    week_start,
    NULL AS total_coins_launched,
    NULL AS total_coins_launched_last_7_days,
    NULL AS total_coins_migration,
    NULL AS total_coins_migration_last_7_days,
    NULL AS total_graduation_rate_percent,
    NULL AS last_7_days_graduation_rate_percent,
    launch_count,
    migration_count,
    CASE 
        WHEN launch_count > 0 
        THEN CONCAT(CAST(ROUND((migration_count * 100.0 / launch_count), 2) AS VARCHAR), '%')
        ELSE '0.00%'
    END AS weekly_graduation_rate_percent
FROM weekly_combined
ORDER BY data_type, week_start;