-- part of a query repo
-- query name: Trends
-- query link: https://dune.com/queries/5555359


WITH
  graduated_tokens AS (
    SELECT DISTINCT
      account_base_mint AS token_mint_address,
      call_block_time AS block_time,
      TRUE AS is_graduated
    FROM
      meteora_solana.dynamic_bonding_curve_call_migration_damm_v2
    WHERE
      call_block_time >= DATE '2024-05-31'
      AND account_config in (
        '7UMR4yEaVYsQGbQGvxNUypFmPn15GkzVmwUEpUFJUPPX',
        '7UNpFBfTdWrcfS7aBQzEaPgZCfPJe8BDgHzwmWUZaMaF',
        '7UQpAg2GfvwnBhuNAF5g9ujjDmkq7rPnF7Xogs4xE9AA',
        '7UP2hcAoYvyzumQv3BtvmXDCQk2WoqMEXKym8cCdLAh6'
      )
  ),
  non_graduated_tokens AS (
    SELECT DISTINCT
      base_mint AS token_mint_address,
      evt_block_time AS block_time,
      FALSE AS is_graduated
    FROM
      meteora_solana.dynamic_bonding_curve_evt_evtinitializepool
    WHERE
      evt_block_time >= DATE '2024-05-31'
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
  all_tokens AS (
    SELECT * FROM graduated_tokens
    UNION ALL
    SELECT * FROM non_graduated_tokens
  ),
  token_info AS (
    SELECT
      token_mint_address,
      symbol,
      name
    FROM
      tokens_solana.fungible
    WHERE
      token_mint_address IN (
        SELECT
          token_mint_address
        FROM
          all_tokens
      )
  ),
  dev AS (
    SELECT
      account_creator as deployer,
      account_base_mint as token_mint_address
    FROM
      meteora_solana.dynamic_bonding_curve_call_initialize_virtual_pool_with_token2022
    WHERE
      account_base_mint IN (
        SELECT
          token_mint_address
        FROM
          all_tokens
      )
  ),
  holders AS (
    SELECT
      token_mint_address,
      COUNT(DISTINCT token_balance_owner) AS holder_count
    FROM
      solana_utils.latest_balances
    WHERE
      token_mint_address IN (
        SELECT
          token_mint_address
        FROM
          all_tokens
      )
      AND token_balance <> 0
    GROUP BY
      token_mint_address
  ),
  token_supplies AS (
    SELECT
      token_mint_address,
      COALESCE(SUM(token_balance), 0) AS total_supply
    FROM
      solana_utils.latest_balances
    WHERE
      token_mint_address IN (
        SELECT
          token_mint_address
        FROM
          all_tokens
      )
      AND token_balance > 0
    GROUP BY
      token_mint_address
  ),
  latest_prices AS (
    SELECT
      contract_address AS token_mint_address,
      price,
      ROW_NUMBER() OVER (
        PARTITION BY
          contract_address
        ORDER BY
          hour DESC
      ) AS rn
    FROM
      dex_solana.price_hour
    WHERE
      contract_address IN (
        SELECT
          token_mint_address
        FROM
          all_tokens
      )
  )
SELECT
  at.block_time AS launch_block_time,
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
    ELSE ti.name
  END AS name,
  ti.symbol,
  CONCAT(
    '<a href = https://axiom.trade/t/',
    at.token_mint_address,
    '/@oladeeayo target=_blank">',
    at.token_mint_address,
    '</a>'
  ) AS token_address,
  COALESCE(h.holder_count, 0) AS holder_count,
  CASE
    WHEN at.is_graduated THEN '✅'
    ELSE '❌'
  END AS graduated,
  dev.deployer as deployer
  -- CASE
  --   WHEN h.holder_count BETWEEN 1 AND 10 AND COALESCE(lp.price * ts.total_supply, 0) > 500000000 THEN CAST(NULL AS double)
  --   ELSE COALESCE(lp.price * ts.total_supply, 0)
  -- END AS market_cap
FROM
  all_tokens AS at
  LEFT JOIN token_info AS ti ON at.token_mint_address = ti.token_mint_address
  LEFT JOIN dev on at.token_mint_address = dev.token_mint_address
  LEFT JOIN holders AS h ON at.token_mint_address = h.token_mint_address
  LEFT JOIN latest_prices AS lp ON at.token_mint_address = lp.token_mint_address
  AND lp.rn = 1
  LEFT JOIN token_supplies AS ts ON at.token_mint_address = ts.token_mint_address
ORDER BY
  holder_count DESC
  -- CASE 
  --   WHEN at.is_graduated THEN COALESCE(lp.price * ts.total_supply, 0)
  --   ELSE 0
  -- END DESC