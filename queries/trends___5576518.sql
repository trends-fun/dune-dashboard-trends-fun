-- part of a query repo
-- query name: Trends
-- query link: https://dune.com/queries/5576518


WITH
-- Get current SOL price
sol_price AS (
    SELECT price as current_sol_price
    FROM prices.minute
    WHERE blockchain='solana'
    AND symbol='SOL'
    AND contract_address=0x0000000000000000000000000000000000000000
    ORDER BY timestamp DESC
    LIMIT 1
),

-- Get all graduated tokens
graduated_tokens AS (
    SELECT DISTINCT
        account_base_mint AS token_mint_address,
        call_tx_signer AS tx_signer,
        call_block_time AS block_time,
        TRUE AS is_graduated
    FROM meteora_solana.dynamic_bonding_curve_call_migration_damm_v2
    WHERE call_block_time >= DATE '2024-05-31'
        AND account_config in (
             '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
             '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
             '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
             '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
        )
),

-- Get all non-graduated tokens
non_graduated_tokens AS (
    SELECT DISTINCT
        base_mint AS token_mint_address,
        evt_tx_signer AS tx_signer,
        evt_block_time AS block_time,
        FALSE AS is_graduated
    FROM meteora_solana.dynamic_bonding_curve_evt_evtinitializepool
    WHERE evt_block_time >= DATE '2024-05-31'
        AND config in (
            '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
            '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
            '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
            '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
        )
        AND base_mint NOT IN (
            SELECT token_mint_address FROM graduated_tokens
        )
),

-- Combine all tokens
all_tokens AS (
    SELECT * FROM graduated_tokens
    UNION ALL
    SELECT * FROM non_graduated_tokens
),

-- Get token info from tokens table
token_info AS (
    SELECT
        at.token_mint_address,
        at.tx_signer,
        at.block_time,
        at.is_graduated,
        tf.name,
        tf.symbol
    FROM all_tokens at
    LEFT JOIN tokens_solana.fungible tf ON at.token_mint_address = tf.token_mint_address
),

-- Get pools for graduated tokens (directly from migration table)
pools AS (
    SELECT DISTINCT
        account_pool AS pool_address,
        account_base_mint AS token_mint_address
    FROM meteora_solana.dynamic_bonding_curve_call_migration_damm_v2
    WHERE call_block_time >= DATE '2024-05-31'
        AND account_config in (
            '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
            '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
            '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
            '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
        )
),

-- Calculate internal trading volume (for non-graduated tokens)
internal_trading_volume AS (
    SELECT
        ti.token_mint_address,
        SUM(
            CASE
                WHEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.actual_input_amount') AS BIGINT) >
                     CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS BIGINT)
                THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS BIGINT) / 1000000000.0
                ELSE CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.actual_input_amount') AS BIGINT) / 1000000000.0
            END
        ) AS trading_volume_sol
    FROM token_info ti
    JOIN meteora_solana.dynamic_bonding_curve_evt_evtswap ds 
        ON ti.tx_signer = ds.evt_tx_signer
    WHERE ti.is_graduated = FALSE
        AND ds.config in (
            '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
            '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
            '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
            '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
        )
        AND ds.evt_block_time >= DATE'2025-06-01'
    GROUP BY ti.token_mint_address
),

-- Calculate external trading volume (for graduated tokens)
external_trading_volume AS (
    SELECT
        ti.token_mint_address,
        SUM(
            CASE
                WHEN es.actual_amount_in > CAST(JSON_EXTRACT_SCALAR(es.swap_result, '$.SwapResult.output_amount') AS BIGINT)
                THEN CAST(JSON_EXTRACT_SCALAR(es.swap_result, '$.SwapResult.output_amount') AS BIGINT) / 1000000000.0
                ELSE es.actual_amount_in / 1000000000.0
            END
        ) AS trading_volume_sol
    FROM token_info ti
    JOIN pools p ON ti.token_mint_address = p.token_mint_address
    JOIN meteora_solana.cp_amm_evt_evtswap es ON p.pool_address = es.pool
    WHERE ti.is_graduated = TRUE
        AND es.evt_block_time >= DATE'2025-06-01'
    GROUP BY ti.token_mint_address
),

-- Combine trading volumes
combined_trading_volume AS (
    SELECT token_mint_address, trading_volume_sol FROM internal_trading_volume
    UNION ALL
    SELECT token_mint_address, trading_volume_sol FROM external_trading_volume
),

-- Process names and calculate aggregated data
name_aggregated_data AS (
    SELECT
        CASE 
            WHEN ti.name IS NOT NULL AND ti.name LIKE '@%' THEN
                CONCAT(
                    '<a href="https://twitter.com/',
                    SUBSTRING(ti.name, 2),
                    '" target="_blank">',
                    ti.name,
                    '</a>'
                )
            WHEN ti.name IS NOT NULL AND ti.name NOT LIKE '@%' THEN
                CONCAT(
                    '<a href="https://twitter.com/',
                    ti.name,
                    '" target="_blank">',
                    '@', ti.name,
                    '</a>'
                )
            ELSE COALESCE(ti.name, 'Unknown')
        END AS processed_name,
        ti.name AS original_name,
        COUNT(ti.token_mint_address) AS token_count,
        SUM(COALESCE(ctv.trading_volume_sol, 0)) AS total_trading_volume_sol,
        SUM(COALESCE(ctv.trading_volume_sol, 0)) * sp.current_sol_price AS total_trading_volume_usd
    FROM token_info ti
    LEFT JOIN combined_trading_volume ctv ON ti.token_mint_address = ctv.token_mint_address
    CROSS JOIN sol_price sp
    GROUP BY ti.name, sp.current_sol_price
),

-- Get total unique name count
total_name_count AS (
     SELECT COUNT(DISTINCT name) - 393 AS total_unique_names 
    FROM token_info
    WHERE name IS NOT NULL
),

-- Get recent 7 days unique name count
recent_7_days_name_count AS (
    SELECT COUNT(DISTINCT name) AS recent_7_days_unique_names
    FROM token_info
    WHERE name IS NOT NULL
        AND block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
)

-- Main query: Return detailed results ordered by trading volume
SELECT
    nad.processed_name,
    nad.token_count,
    nad.total_trading_volume_sol,
    nad.total_trading_volume_usd,
    tnc.total_unique_names,
    r7nc.recent_7_days_unique_names
FROM name_aggregated_data nad
CROSS JOIN total_name_count tnc
CROSS JOIN recent_7_days_name_count r7nc
ORDER BY nad.total_trading_volume_usd DESC;