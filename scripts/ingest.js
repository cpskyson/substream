#!/usr/bin/env node
const readline = require("readline");
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const upsertPoolSql = `
  insert into pools (
    pool_id,
    token_a_mint,
    token_b_mint,
    token_a_symbol,
    token_b_symbol,
    token_a_uri,
    token_b_uri,
    token_a_decimals,
    token_b_decimals,
    fee_tier_bps,
    token_a_balance,
    token_b_balance,
    tvl_estimate,
    tvl_usd,
    token_a_price_usd,
    token_b_price_usd,
    updated_slot
  )
  values (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
  )
  on conflict (pool_id) do update set
    token_a_mint = case
      when excluded.updated_slot >= coalesce(pools.updated_slot, 0) then excluded.token_a_mint
      else pools.token_a_mint
    end,
    token_b_mint = case
      when excluded.updated_slot >= coalesce(pools.updated_slot, 0) then excluded.token_b_mint
      else pools.token_b_mint
    end,
    token_a_symbol = case
      when coalesce(nullif(excluded.token_a_symbol, ''), '') = '' then pools.token_a_symbol
      when coalesce(excluded.token_a_mint, '') <> ''
        and excluded.token_a_symbol = substring(excluded.token_a_mint from 1 for 4) || '..' || substring(excluded.token_a_mint from greatest(char_length(excluded.token_a_mint)-3, 1) for 4)
        and coalesce(pools.token_a_symbol, '') <> ''
        and pools.token_a_symbol <> substring(pools.token_a_mint from 1 for 4) || '..' || substring(pools.token_a_mint from greatest(char_length(pools.token_a_mint)-3, 1) for 4)
        then pools.token_a_symbol
      else excluded.token_a_symbol
    end,
    token_b_symbol = case
      when coalesce(nullif(excluded.token_b_symbol, ''), '') = '' then pools.token_b_symbol
      when coalesce(excluded.token_b_mint, '') <> ''
        and excluded.token_b_symbol = substring(excluded.token_b_mint from 1 for 4) || '..' || substring(excluded.token_b_mint from greatest(char_length(excluded.token_b_mint)-3, 1) for 4)
        and coalesce(pools.token_b_symbol, '') <> ''
        and pools.token_b_symbol <> substring(pools.token_b_mint from 1 for 4) || '..' || substring(pools.token_b_mint from greatest(char_length(pools.token_b_mint)-3, 1) for 4)
        then pools.token_b_symbol
      else excluded.token_b_symbol
    end,
    token_a_uri = coalesce(nullif(excluded.token_a_uri, ''), pools.token_a_uri),
    token_b_uri = coalesce(nullif(excluded.token_b_uri, ''), pools.token_b_uri),
    token_a_decimals = case
      when excluded.updated_slot >= coalesce(pools.updated_slot, 0) and excluded.token_a_decimals > 0
        then excluded.token_a_decimals
      else pools.token_a_decimals
    end,
    token_b_decimals = case
      when excluded.updated_slot >= coalesce(pools.updated_slot, 0) and excluded.token_b_decimals > 0
        then excluded.token_b_decimals
      else pools.token_b_decimals
    end,
    fee_tier_bps = case
      when excluded.updated_slot >= coalesce(pools.updated_slot, 0) and excluded.fee_tier_bps > 0
        then excluded.fee_tier_bps
      else pools.fee_tier_bps
    end,
    token_a_balance = case
      when excluded.updated_slot >= coalesce(pools.updated_slot, 0) then excluded.token_a_balance
      else pools.token_a_balance
    end,
    token_b_balance = case
      when excluded.updated_slot >= coalesce(pools.updated_slot, 0) then excluded.token_b_balance
      else pools.token_b_balance
    end,
    tvl_estimate = case
      when excluded.updated_slot >= coalesce(pools.updated_slot, 0) then excluded.tvl_estimate
      else pools.tvl_estimate
    end,
    tvl_usd = case
      when excluded.updated_slot >= coalesce(pools.updated_slot, 0) then excluded.tvl_usd
      else pools.tvl_usd
    end,
    token_a_price_usd = case
      when excluded.updated_slot >= coalesce(pools.updated_slot, 0) then excluded.token_a_price_usd
      else pools.token_a_price_usd
    end,
    token_b_price_usd = case
      when excluded.updated_slot >= coalesce(pools.updated_slot, 0) then excluded.token_b_price_usd
      else pools.token_b_price_usd
    end,
    updated_slot = greatest(coalesce(pools.updated_slot, 0), excluded.updated_slot)
`;

const upsertPositionSql = `
  insert into positions (
    position_id,
    pool_id,
    owner,
    token_a_mint,
    token_b_mint,
    token_a_symbol,
    token_b_symbol,
    token_a_uri,
    token_b_uri,
    token_a_decimals,
    token_b_decimals,
    fee_tier_bps,
    token_a_balance,
    token_b_balance,
    pool_value,
    updated_slot
  )
  values (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
  )
  on conflict (position_id) do update set
    pool_id = case
      when excluded.updated_slot >= coalesce(positions.updated_slot, 0) then excluded.pool_id
      else positions.pool_id
    end,
    owner = coalesce(nullif(excluded.owner, ''), positions.owner),
    token_a_mint = case
      when excluded.updated_slot >= coalesce(positions.updated_slot, 0) then excluded.token_a_mint
      else positions.token_a_mint
    end,
    token_b_mint = case
      when excluded.updated_slot >= coalesce(positions.updated_slot, 0) then excluded.token_b_mint
      else positions.token_b_mint
    end,
    token_a_symbol = case
      when coalesce(nullif(excluded.token_a_symbol, ''), '') = '' then positions.token_a_symbol
      when coalesce(excluded.token_a_mint, '') <> ''
        and excluded.token_a_symbol = substring(excluded.token_a_mint from 1 for 4) || '..' || substring(excluded.token_a_mint from greatest(char_length(excluded.token_a_mint)-3, 1) for 4)
        and coalesce(positions.token_a_symbol, '') <> ''
        and positions.token_a_symbol <> substring(positions.token_a_mint from 1 for 4) || '..' || substring(positions.token_a_mint from greatest(char_length(positions.token_a_mint)-3, 1) for 4)
        then positions.token_a_symbol
      else excluded.token_a_symbol
    end,
    token_b_symbol = case
      when coalesce(nullif(excluded.token_b_symbol, ''), '') = '' then positions.token_b_symbol
      when coalesce(excluded.token_b_mint, '') <> ''
        and excluded.token_b_symbol = substring(excluded.token_b_mint from 1 for 4) || '..' || substring(excluded.token_b_mint from greatest(char_length(excluded.token_b_mint)-3, 1) for 4)
        and coalesce(positions.token_b_symbol, '') <> ''
        and positions.token_b_symbol <> substring(positions.token_b_mint from 1 for 4) || '..' || substring(positions.token_b_mint from greatest(char_length(positions.token_b_mint)-3, 1) for 4)
        then positions.token_b_symbol
      else excluded.token_b_symbol
    end,
    token_a_uri = coalesce(nullif(excluded.token_a_uri, ''), positions.token_a_uri),
    token_b_uri = coalesce(nullif(excluded.token_b_uri, ''), positions.token_b_uri),
    token_a_decimals = case
      when excluded.updated_slot >= coalesce(positions.updated_slot, 0) and excluded.token_a_decimals > 0
        then excluded.token_a_decimals
      else positions.token_a_decimals
    end,
    token_b_decimals = case
      when excluded.updated_slot >= coalesce(positions.updated_slot, 0) and excluded.token_b_decimals > 0
        then excluded.token_b_decimals
      else positions.token_b_decimals
    end,
    fee_tier_bps = case
      when excluded.updated_slot >= coalesce(positions.updated_slot, 0) and excluded.fee_tier_bps > 0
        then excluded.fee_tier_bps
      else positions.fee_tier_bps
    end,
    token_a_balance = case
      when excluded.updated_slot >= coalesce(positions.updated_slot, 0) then excluded.token_a_balance
      else positions.token_a_balance
    end,
    token_b_balance = case
      when excluded.updated_slot >= coalesce(positions.updated_slot, 0) then excluded.token_b_balance
      else positions.token_b_balance
    end,
    pool_value = case
      when excluded.updated_slot >= coalesce(positions.updated_slot, 0) then excluded.pool_value
      else positions.pool_value
    end,
    updated_slot = greatest(coalesce(positions.updated_slot, 0), excluded.updated_slot)
`;

const upsertHourlySql = `
  insert into pool_hourly (
    pool_id,
    hour,
    volume_a,
    volume_b,
    fees_a,
    fees_b
  )
  values ($1,$2,$3,$4,$5,$6)
  on conflict (pool_id, hour) do update set
    volume_a = pool_hourly.volume_a + excluded.volume_a,
    volume_b = pool_hourly.volume_b + excluded.volume_b,
    fees_a = pool_hourly.fees_a + excluded.fees_a,
    fees_b = pool_hourly.fees_b + excluded.fees_b
`;

function toNumber(value) {
  if (value === null || value === undefined) return 0;
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function toString(value) {
  if (value === null || value === undefined) return "";
  return String(value);
}

function getField(obj, camel, snake) {
  if (obj && Object.prototype.hasOwnProperty.call(obj, camel)) {
    return obj[camel];
  }
  if (obj && snake && Object.prototype.hasOwnProperty.call(obj, snake)) {
    return obj[snake];
  }
  return undefined;
}

async function handleAmmUpdates(payload) {
  const data = payload["@data"] || payload.data || {};
  const slot = toString(data.slot || payload["@block"]);

  const pools = data.pools || [];
  for (const p of pools) {
    const values = [
      toString(getField(p, "poolId", "pool_id")),
      toString(getField(p, "tokenAMint", "token_a_mint")),
      toString(getField(p, "tokenBMint", "token_b_mint")),
      toString(getField(p, "tokenASymbol", "token_a_symbol")),
      toString(getField(p, "tokenBSymbol", "token_b_symbol")),
      toString(getField(p, "tokenAUri", "token_a_uri")),
      toString(getField(p, "tokenBUri", "token_b_uri")),
      toNumber(getField(p, "tokenADecimals", "token_a_decimals")),
      toNumber(getField(p, "tokenBDecimals", "token_b_decimals")),
      toNumber(getField(p, "feeTierBps", "fee_tier_bps")),
      toNumber(getField(p, "tokenABalance", "token_a_balance")),
      toNumber(getField(p, "tokenBBalance", "token_b_balance")),
      toNumber(getField(p, "tvlEstimate", "tvl_estimate")),
      toNumber(getField(p, "tvlUsd", "tvl_usd")),
      toNumber(getField(p, "tokenAPriceUsd", "token_a_price_usd")),
      toNumber(getField(p, "tokenBPriceUsd", "token_b_price_usd")),
      slot,
    ];
    if (!values[0]) continue;
    await pool.query(upsertPoolSql, values);
  }

  const positions = data.positions || [];
  for (const p of positions) {
    const values = [
      toString(getField(p, "positionId", "position_id")),
      toString(getField(p, "poolId", "pool_id")),
      toString(getField(p, "owner", "owner")),
      toString(getField(p, "tokenAMint", "token_a_mint")),
      toString(getField(p, "tokenBMint", "token_b_mint")),
      toString(getField(p, "tokenASymbol", "token_a_symbol")),
      toString(getField(p, "tokenBSymbol", "token_b_symbol")),
      toString(getField(p, "tokenAUri", "token_a_uri")),
      toString(getField(p, "tokenBUri", "token_b_uri")),
      toNumber(getField(p, "tokenADecimals", "token_a_decimals")),
      toNumber(getField(p, "tokenBDecimals", "token_b_decimals")),
      toNumber(getField(p, "feeTierBps", "fee_tier_bps")),
      toNumber(getField(p, "tokenABalance", "token_a_balance")),
      toNumber(getField(p, "tokenBBalance", "token_b_balance")),
      toNumber(getField(p, "poolValue", "pool_value")),
      slot,
    ];
    if (!values[0]) continue;
    await pool.query(upsertPositionSql, values);
  }

  if (slot) {
    console.error(
      `[ingest] ${payload["@module"] || payload.module} slot=${slot} pools=${pools.length} positions=${positions.length}`
    );
  }
}

async function handlePoolHourly(payload) {
  const data = payload["@data"] || payload.data || {};
  const slot = toString(data.slot || payload["@block"]);
  const poolHours = data.poolHours || data.pool_hours || [];
  for (const h of poolHours) {
    const values = [
      toString(getField(h, "poolId", "pool_id")),
      toNumber(getField(h, "hour", "hour")),
      toNumber(getField(h, "volumeA", "volume_a")),
      toNumber(getField(h, "volumeB", "volume_b")),
      toNumber(getField(h, "feesA", "fees_a")),
      toNumber(getField(h, "feesB", "fees_b")),
    ];
    if (!values[0]) continue;
    await pool.query(upsertHourlySql, values);
  }

  if (slot) {
    console.error(
      `[ingest] ${payload["@module"] || payload.module} slot=${slot} pool_hours=${poolHours.length}`
    );
  }
}

async function processLine(line) {
  const trimmed = line.trim();
  if (!trimmed) return;
  if (!trimmed.startsWith("{")) {
    return;
  }
  let payload;
  try {
    payload = JSON.parse(trimmed);
  } catch (err) {
    return;
  }

  const moduleName = payload["@module"] || payload.module;
  if (moduleName === "map_pool_hourly") {
    await handlePoolHourly(payload);
    return;
  }

  if (
    moduleName === "map_amm_updates" ||
    moduleName === "map_my_data" ||
    moduleName === "map_amm_priced"
  ) {
    await handleAmmUpdates(payload);
  }
}

async function main() {
  const rl = readline.createInterface({ input: process.stdin });
  let chain = Promise.resolve();

  rl.on("line", (line) => {
    chain = chain.then(() => processLine(line)).catch((err) => {
      console.error(err);
    });
  });

  rl.on("close", async () => {
    await chain;
    await pool.end();
  });
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
