-- part of a query repo
-- query name: Trends
-- query link: https://dune.com/queries/5555525
-- purpose: Estimate ecosystem market cap by combining internal and migrated tokens, including priced supply-based cap and fallback assumptions for missing prices.


WITH internal_tokens AS (
  SELECT DISTINCT 
    base_mint AS token_mint_address,
    evt_tx_signer AS tx_signer,
    evt_block_time AS block_time,
    'internal' AS token_type
  FROM meteora_solana.dynamic_bonding_curve_evt_evtinitializepool 
  WHERE config in (
    '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
    '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
    '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
    '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
  )
    AND evt_block_date >= DATE '2025-06-01'
),

external_tokens AS (
  SELECT DISTINCT 
    account_base_mint AS token_mint_address,
    call_tx_signer AS tx_signer,
    call_block_time AS block_time,
    'external' AS token_type
  FROM meteora_solana.dynamic_bonding_curve_call_migration_damm_v2 
  WHERE account_config in (
    '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
    '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
    '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
    '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
  )
    AND call_block_date >= DATE '2025-06-01'
),

new_curve_tokens AS (
  SELECT
    base_mint AS token_mint_address,
    CAST(NULL AS VARCHAR) AS tx_signer,
    MIN(block_time) AS block_time,
    'internal' AS token_type
  FROM
    dune.data_watcher.result_bonding_curve_swap_events
  GROUP BY
    base_mint
),

all_tokens AS (
  SELECT * FROM internal_tokens
  UNION ALL
  SELECT * FROM external_tokens
  UNION ALL
  SELECT * FROM new_curve_tokens
),

token_info AS (
  SELECT token_mint_address, symbol
  FROM tokens_solana.fungible
  WHERE token_mint_address IN (SELECT token_mint_address FROM all_tokens)
),

daily_prices_raw AS (
  SELECT 
    contract_address AS token_mint_address,
    DATE(hour) AS price_date,
    AVG(price) AS avg_daily_price,
    MAX(price) AS max_daily_price,
    MIN(price) AS min_daily_price,
    STDDEV(price) AS price_stddev,
    COUNT(*) AS price_points
  FROM dex_solana.price_hour
  WHERE 
    contract_address IN (SELECT token_mint_address FROM external_tokens)  -- only graduated
    AND price > 0 AND price < 10000
  GROUP BY contract_address, DATE(hour)
),

daily_prices AS (
  SELECT 
    *,
    LAG(avg_daily_price) OVER (PARTITION BY token_mint_address ORDER BY price_date) AS prev_day_price,
    CASE 
      WHEN LAG(avg_daily_price) OVER (PARTITION BY token_mint_address ORDER BY price_date) > 0 
      THEN ABS((avg_daily_price - LAG(avg_daily_price) OVER (PARTITION BY token_mint_address ORDER BY price_date)) / LAG(avg_daily_price) OVER (PARTITION BY token_mint_address ORDER BY price_date))
      ELSE NULL
    END AS daily_price_change_ratio
  FROM daily_prices_raw
),

filtered_daily_prices AS (
  SELECT *
  FROM daily_prices
  WHERE 
    price_points >= 2
    AND (max_daily_price / min_daily_price) <= 50
    AND (daily_price_change_ratio IS NULL OR daily_price_change_ratio <= 10)
    AND (price_stddev IS NULL OR price_stddev / avg_daily_price <= 5)
),

latest_avg_prices AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY token_mint_address ORDER BY price_date DESC) AS rn
    FROM filtered_daily_prices
  ) sub
  WHERE rn = 1
),

token_supplies AS (
  SELECT 
    token_mint_address,
    COALESCE(SUM(token_balance), 0) AS total_supply
  FROM solana_utils.latest_balances
  WHERE 
    token_mint_address IN (SELECT token_mint_address FROM external_tokens)  -- only migrated
    AND token_balance > 0
  GROUP BY token_mint_address
),

token_data AS (
  SELECT
    at.token_mint_address,
    at.token_type,
    at.tx_signer AS deployer,
    at.block_time AS launch_block_time,
    ti.symbol,
    ts.total_supply,
    lp.avg_daily_price,
    CASE 
      WHEN at.token_type = 'external' AND lp.avg_daily_price IS NOT NULL AND ts.total_supply IS NOT NULL 
      THEN (lp.avg_daily_price * ts.total_supply)
      ELSE NULL
    END AS calculated_market_cap
  FROM all_tokens at
  LEFT JOIN token_info ti ON at.token_mint_address = ti.token_mint_address
  LEFT JOIN latest_avg_prices lp ON at.token_mint_address = lp.token_mint_address AND at.token_type = 'external'  -- only join migrated prices
  LEFT JOIN token_supplies ts ON at.token_mint_address = ts.token_mint_address AND at.token_type = 'external'  -- only join migrated supplies
),

summary AS (
  SELECT
    -- bonding curve
    COUNT(CASE WHEN token_type = 'internal' THEN 1 END) AS internal_total_count,
    COUNT(CASE WHEN token_type = 'internal' AND calculated_market_cap IS NOT NULL THEN 1 END) AS internal_with_price_count,
    COUNT(CASE WHEN token_type = 'internal' AND calculated_market_cap IS NULL THEN 1 END) AS internal_without_price_count,
    COALESCE(SUM(CASE WHEN token_type = 'internal' AND calculated_market_cap IS NOT NULL THEN calculated_market_cap END), 0) AS internal_calculated_market_cap,
    
    -- migrated
    COUNT(CASE WHEN token_type = 'external' THEN 1 END) AS external_total_count,
    COUNT(CASE WHEN token_type = 'external' AND calculated_market_cap IS NOT NULL THEN 1 END) AS external_with_price_count,
    COUNT(CASE WHEN token_type = 'external' AND calculated_market_cap IS NULL THEN 1 END) AS external_without_price_count,
    COALESCE(SUM(CASE WHEN token_type = 'external' AND calculated_market_cap IS NOT NULL THEN calculated_market_cap END), 0) AS external_calculated_market_cap
  FROM token_data
)

SELECT
  -- bonding curve
  internal_total_count,
  internal_with_price_count,
  internal_without_price_count,
  internal_calculated_market_cap,
  
  -- migrated
  external_total_count,
  external_with_price_count,
  external_without_price_count,
  external_calculated_market_cap,
  
  -- market cap of 4 parts
  internal_calculated_market_cap AS part1_internal_with_price,
  (internal_without_price_count / 2 * 4500) AS part2_internal_half_4500usd,
  (internal_without_price_count - internal_without_price_count / 2) * 36 AS part3_internal_half_36usd,
  external_without_price_count * 150000 AS part4_external_without_price_150k,
  
  -- total market cap
  (
    internal_calculated_market_cap + 
    (internal_without_price_count / 2 * 4500) + 
    ((internal_without_price_count - internal_without_price_count / 2) * 36) + 
    (external_without_price_count * 150000) + 
    external_calculated_market_cap
  ) AS total_market_cap
FROM summary;
