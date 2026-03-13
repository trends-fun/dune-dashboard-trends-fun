-- part of a query repo
-- query name: bonding curve swap events
-- query link: https://dune.com/queries/6811456
-- purpose: Decode bonding-curve SwapEvent instruction payloads from solana.instruction_calls into structured swap fields for downstream analytics.


SELECT
    tx_id,
    block_time,

    -- payload
    to_base58(varbinary_substring(data, 17, 32))  AS pool,
    to_base58(varbinary_substring(data, 49, 32))  AS base_mint,
    to_base58(varbinary_substring(data, 81, 32))  AS trader,

    varbinary_to_bigint(varbinary_substring(data, 113, 1)) AS trade_direction,

    varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 114, 8))) AS amount_in,
    varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 122, 8))) AS actual_amount_in,
    varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 130, 8))) AS min_amount_out,
    varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 138, 8))) AS actual_amount_out,
    varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 146, 8))) AS creator_fee,
    varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 154, 8))) AS protocol_fee,
    varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 162, 8))) AS referral_fee,
    varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 170, 8))) AS base_reserve,
    varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 178, 8))) AS quote_reserve,
    varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 186, 8))) AS virtual_base_reserve,
    varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 194, 8))) AS virtual_quote_reserve

FROM solana.instruction_calls

WHERE executing_account = 'CURVEmPpijXDTNdqrA9PGP1io2rkgiVXH26xdXVGLLfz'

-- Anchor emit_cpi
AND varbinary_substring(data,1,8) = 0xe445a52e51cb9a1d

-- SwapEvent
AND varbinary_substring(data,9,8) = 0x40c6cde8260871e2

AND block_date >= TIMESTAMP '2026-03-04 00:00:00'   -- 协议上线时间

ORDER BY block_date DESC
