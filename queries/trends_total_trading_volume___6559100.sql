-- part of a query repo
-- query name: Trends Total Trading Volume
-- query link: https://dune.com/queries/6559100


WITH migration_pools AS (
  SELECT DISTINCT account_pool
  FROM meteora_solana.dynamic_bonding_curve_call_migration_damm_v2
  WHERE account_config IN (
    '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
    '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
    '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
    '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
  )
),
events AS (
  -- DBC swaps (configs filtered directly)
  SELECT
    DATE_TRUNC('minute', evt_block_time) AS event_minute,
    CASE
      WHEN trade_direction = 0
        THEN TRY_CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS DECIMAL(38,0)) / 1e9
      ELSE
        TRY_CAST(amount_in AS DECIMAL(38,0)) / 1e9
    END AS volume_sol
  FROM meteora_solana.dynamic_bonding_curve_evt_evtswap
  WHERE config IN (
    '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
    '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
    '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
    '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
  )

  UNION ALL

  -- DAMM v2 swaps (restricted to pools known to have migrated under those configs)
  SELECT
    DATE_TRUNC('minute', evt_block_time) AS event_minute,
    CASE
      WHEN trade_direction = 0
        THEN TRY_CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS DECIMAL(38,0)) / 1e9
      ELSE
        TRY_CAST(JSON_EXTRACT_SCALAR(params, '$.SwapParameters.amount_in') AS DECIMAL(38,0)) / 1e9
    END AS volume_sol
  FROM meteora_solana.cp_amm_evt_evtswap e
  WHERE EXISTS (
    SELECT 1
    FROM migration_pools m
    WHERE m.account_pool = e.pool
  )

  UNION ALL

  -- DAMM v2 swap2
  SELECT
    DATE_TRUNC('minute', evt_block_time) AS event_minute,
    CASE
      WHEN trade_direction = 0
        THEN TRY_CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult2.output_amount') AS DECIMAL(38,0)) / 1e9
      ELSE
        TRY_CAST(JSON_EXTRACT_SCALAR(params, '$.SwapParameters2.amount_0') AS DECIMAL(38,0)) / 1e9
    END AS volume_sol
  FROM meteora_solana.cp_amm_evt_evtswap2 e
  WHERE EXISTS (
    SELECT 1
    FROM migration_pools m
    WHERE m.account_pool = e.pool
  )
)
SELECT
  CAST(SUM(volume_sol) AS DOUBLE) AS trends_total_volume_sol,
  CAST(SUM(volume_sol * COALESCE(p.price, 0)) AS DOUBLE) AS trends_total_volume_usd
FROM events ev
LEFT JOIN prices.usd p
  ON p.blockchain = 'solana'
 AND p.contract_address = FROM_BASE58('So11111111111111111111111111111111111111112')
 AND p.minute = ev.event_minute;