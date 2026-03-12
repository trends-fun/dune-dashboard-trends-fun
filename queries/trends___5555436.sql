-- part of a query repo
-- query name: Trends
-- query link: https://dune.com/queries/5555436
-- purpose: Produce total/7-day trading volume and active-wallet metrics (SOL/USD), plus weekly wallet and trading trends across pre/post-migration activity.


WITH sol_price AS (
    SELECT price as current_sol_price FROM prices.minute
    WHERE blockchain='solana'
    AND symbol='SOL'
    AND contract_address=0x0000000000000000000000000000000000000000
    ORDER BY timestamp DESC
    LIMIT 1
),
pools AS (
    SELECT DISTINCT account_pool as pool_address
    FROM meteora_solana.dynamic_bonding_curve_call_migration_damm_v2
    WHERE account_config in (
        '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
        '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
        '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
        '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
    )
        AND call_block_date >= DATE'2025-06-02'
),
internal_swap_events AS (
    SELECT
        evt_tx_signer,
        evt_block_time,
        CASE
            WHEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.actual_input_amount') AS BIGINT) >
                 CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS BIGINT)
            THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS BIGINT) / 1000000000.0
            ELSE CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.actual_input_amount') AS BIGINT) / 1000000000.0
        END as trade_amount_sol
    FROM meteora_solana.dynamic_bonding_curve_evt_evtswap
    WHERE config in (
        '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
        '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
        '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
        '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
    )
        AND evt_block_time >= DATE'2025-06-01'

    UNION ALL

    SELECT
        trader AS evt_tx_signer,
        block_time AS evt_block_time,
        CASE
            WHEN TRY_CAST(actual_amount_in AS BIGINT) > TRY_CAST(actual_amount_out AS BIGINT)
            THEN TRY_CAST(actual_amount_out AS BIGINT) / 1000000000.0
            ELSE TRY_CAST(actual_amount_in AS BIGINT) / 1000000000.0
        END as trade_amount_sol
    FROM dune.data_watcher.result_bonding_curve_swap_events
    WHERE block_time >= DATE'2025-06-01'
),
internal_wallet_activity AS (
    SELECT
        evt_tx_signer,
        evt_block_time
    FROM internal_swap_events
),
external_wallet_activity AS (
    SELECT
        evt_tx_signer,
        evt_block_time
    FROM meteora_solana.cp_amm_evt_evtswap
    WHERE pool IN (SELECT pool_address FROM pools)
        AND evt_block_time >= DATE'2025-06-01'
),
all_wallet_activity AS (
    SELECT evt_tx_signer, evt_block_time FROM internal_wallet_activity
    UNION ALL
    SELECT evt_tx_signer, evt_block_time FROM external_wallet_activity
),
filtered_wallets AS (
    SELECT
        evt_tx_signer
    FROM all_wallet_activity
    GROUP BY evt_tx_signer
    HAVING COUNT(evt_tx_signer) < 5000
),
weekly_wallet_stats AS (
    SELECT
        DATE_TRUNC('week', awa.evt_block_time) as week_start,
        COUNT(DISTINCT fw.evt_tx_signer) as weekly_active_wallets
    FROM filtered_wallets fw
    JOIN all_wallet_activity awa ON fw.evt_tx_signer = awa.evt_tx_signer
    WHERE awa.evt_block_time >= DATE'2025-06-01'
    GROUP BY DATE_TRUNC('week', awa.evt_block_time)
    ORDER BY week_start
),
weekly_internal_trades AS (
    SELECT
        DATE_TRUNC('week', evt_block_time) as week_start,
        SUM(trade_amount_sol) as weekly_trade_amount_sol
    FROM internal_swap_events
    GROUP BY DATE_TRUNC('week', evt_block_time)
),
weekly_external_trades AS (
    SELECT
        DATE_TRUNC('week', evt_block_time) as week_start,
        SUM(
            CASE
                WHEN actual_amount_in > CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS BIGINT)
                THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS BIGINT) / 1000000000.0
                ELSE actual_amount_in / 1000000000.0
            END
        ) as weekly_trade_amount_sol
    FROM meteora_solana.cp_amm_evt_evtswap
    WHERE pool IN (SELECT pool_address FROM pools)
        AND evt_block_time >= DATE'2025-06-01'
    GROUP BY DATE_TRUNC('week', evt_block_time)
),
internal_trades AS (
    SELECT
        SUM(trade_amount_sol) as total_trade_amount_sol,
        SUM(
            CASE
                WHEN evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
                THEN trade_amount_sol
                ELSE 0
            END
        ) as last_7_days_trade_amount_sol
    FROM internal_swap_events
    WHERE evt_block_time >= DATE'2024-05-01'
),
external_trades AS (
    SELECT
        SUM(
            CASE
                WHEN actual_amount_in > CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS BIGINT)
                THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS BIGINT) / 1000000000.0
                ELSE actual_amount_in / 1000000000.0
            END
        ) as total_trade_amount_sol,
        SUM(
            CASE
                WHEN evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
                THEN
                    CASE
                        WHEN actual_amount_in > CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS BIGINT)
                        THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS BIGINT) / 1000000000.0
                        ELSE actual_amount_in / 1000000000.0
                    END
                ELSE 0
            END
        ) as last_7_days_trade_amount_sol
    FROM meteora_solana.cp_amm_evt_evtswap
    WHERE pool IN (SELECT pool_address FROM pools)
        AND evt_block_date >= DATE'2025-06-01'
),
wallet_counts AS (
    SELECT
        COUNT(DISTINCT fw.evt_tx_signer) AS total_active_wallets,
        COUNT(DISTINCT CASE WHEN awa.evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY THEN fw.evt_tx_signer ELSE NULL END) AS last_7_days_active_wallets
    FROM filtered_wallets fw
    JOIN all_wallet_activity awa ON fw.evt_tx_signer = awa.evt_tx_signer
),
-- combine post/pre-migrated data
weekly_combined_trades AS (
    SELECT
        COALESCE(wit.week_start, wet.week_start) as week_start,
        COALESCE(wit.weekly_trade_amount_sol, 0) + COALESCE(wet.weekly_trade_amount_sol, 0) as weekly_trade_amount_sol
    FROM weekly_internal_trades wit
    FULL OUTER JOIN weekly_external_trades wet ON wit.week_start = wet.week_start
)
SELECT
    'TOTAL' as data_type,
    NULL as week_start,
    COALESCE(it.total_trade_amount_sol, 0) + COALESCE(et.total_trade_amount_sol, 0) as total_trade_amount_sol,
    COALESCE(it.last_7_days_trade_amount_sol, 0) + COALESCE(et.last_7_days_trade_amount_sol, 0) as last_7_days_trade_amount_sol,
    (COALESCE(it.total_trade_amount_sol, 0) + COALESCE(et.total_trade_amount_sol, 0)) * sp.current_sol_price as total_trade_amount_usd,
    (COALESCE(it.last_7_days_trade_amount_sol, 0) + COALESCE(et.last_7_days_trade_amount_sol, 0)) * sp.current_sol_price as last_7_days_trade_amount_usd,
    wc.total_active_wallets,
    wc.last_7_days_active_wallets,
    NULL as weekly_active_wallets,
    NULL as weekly_trade_amount_sol,
    NULL as weekly_trade_amount_usd
FROM sol_price sp
CROSS JOIN internal_trades it
CROSS JOIN external_trades et
CROSS JOIN wallet_counts wc

UNION ALL

SELECT
    'WEEKLY' as data_type,
    COALESCE(wct.week_start, wws.week_start) as week_start,
    NULL as total_trade_amount_sol,
    NULL as last_7_days_trade_amount_sol,
    NULL as total_trade_amount_usd,
    NULL as last_7_days_trade_amount_usd,
    NULL as total_active_wallets,
    NULL as last_7_days_active_wallets,
    wws.weekly_active_wallets,
    wct.weekly_trade_amount_sol,
    wct.weekly_trade_amount_sol * sp.current_sol_price as weekly_trade_amount_usd
FROM weekly_combined_trades wct
FULL OUTER JOIN weekly_wallet_stats wws ON wct.week_start = wws.week_start
CROSS JOIN sol_price sp
ORDER BY data_type, week_start;