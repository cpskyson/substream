#!/usr/bin/env node
const { Pool } = require("pg");

const METAPLEX_METADATA_PROGRAM_ID =
  "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";

const STATIC_TOKEN_METADATA = Object.freeze({
  So11111111111111111111111111111111111111112: {
    symbol: "SOL",
    decimals: 9,
    uri: "",
  },
});

const DATABASE_URL = process.env.DATABASE_URL;
const SOLANA_RPC_URL = process.env.SOLANA_RPC_URL || "https://api.devnet.solana.com";
const RPC_COMMITMENT = process.env.RPC_COMMITMENT || "confirmed";
const BACKFILL_CONCURRENCY = Math.max(
  1,
  Number(process.env.BACKFILL_CONCURRENCY || 6),
);
const MAX_RPC_RETRIES = Math.max(1, Number(process.env.MAX_RPC_RETRIES || 4));
const RETRY_DELAY_MS = Math.max(100, Number(process.env.RETRY_DELAY_MS || 350));
const DRY_RUN = process.env.DRY_RUN === "1";

if (!DATABASE_URL) {
  console.error("DATABASE_URL is required");
  process.exit(1);
}

const db = new Pool({ connectionString: DATABASE_URL });
let rpcId = 1;

const selectCandidateMintsSql = `
with mint_rows as (
  select token_a_mint as mint, token_a_symbol as symbol, token_a_decimals as decimals, token_a_uri as uri from pools
  union all
  select token_b_mint as mint, token_b_symbol as symbol, token_b_decimals as decimals, token_b_uri as uri from pools
  union all
  select token_a_mint as mint, token_a_symbol as symbol, token_a_decimals as decimals, token_a_uri as uri from positions
  union all
  select token_b_mint as mint, token_b_symbol as symbol, token_b_decimals as decimals, token_b_uri as uri from positions
)
select distinct mint
from mint_rows
where mint is not null
  and mint <> ''
  and (
    coalesce(symbol, '') = ''
    or symbol = substring(mint from 1 for 4) || '..' || substring(mint from greatest(char_length(mint)-3, 1) for 4)
    or coalesce(decimals, 0) = 0
    or coalesce(uri, '') = ''
  )
order by mint
`;

const updatePoolTokenASql = `
update pools
set
  token_a_symbol = case
    when $2 <> '' and (
      coalesce(token_a_symbol, '') = ''
      or token_a_symbol = substring(token_a_mint from 1 for 4) || '..' || substring(token_a_mint from greatest(char_length(token_a_mint)-3, 1) for 4)
    ) then $2
    else token_a_symbol
  end,
  token_a_decimals = case
    when $3 > 0 and coalesce(token_a_decimals, 0) = 0 then $3
    else token_a_decimals
  end,
  token_a_uri = case
    when $4 <> '' and coalesce(token_a_uri, '') = '' then $4
    else token_a_uri
  end
where token_a_mint = $1
  and (
    ($2 <> '' and (
      coalesce(token_a_symbol, '') = ''
      or token_a_symbol = substring(token_a_mint from 1 for 4) || '..' || substring(token_a_mint from greatest(char_length(token_a_mint)-3, 1) for 4)
    ))
    or ($3 > 0 and coalesce(token_a_decimals, 0) = 0)
    or ($4 <> '' and coalesce(token_a_uri, '') = '')
  )
`;

const updatePoolTokenBSql = `
update pools
set
  token_b_symbol = case
    when $2 <> '' and (
      coalesce(token_b_symbol, '') = ''
      or token_b_symbol = substring(token_b_mint from 1 for 4) || '..' || substring(token_b_mint from greatest(char_length(token_b_mint)-3, 1) for 4)
    ) then $2
    else token_b_symbol
  end,
  token_b_decimals = case
    when $3 > 0 and coalesce(token_b_decimals, 0) = 0 then $3
    else token_b_decimals
  end,
  token_b_uri = case
    when $4 <> '' and coalesce(token_b_uri, '') = '' then $4
    else token_b_uri
  end
where token_b_mint = $1
  and (
    ($2 <> '' and (
      coalesce(token_b_symbol, '') = ''
      or token_b_symbol = substring(token_b_mint from 1 for 4) || '..' || substring(token_b_mint from greatest(char_length(token_b_mint)-3, 1) for 4)
    ))
    or ($3 > 0 and coalesce(token_b_decimals, 0) = 0)
    or ($4 <> '' and coalesce(token_b_uri, '') = '')
  )
`;

const updatePositionTokenASql = `
update positions
set
  token_a_symbol = case
    when $2 <> '' and (
      coalesce(token_a_symbol, '') = ''
      or token_a_symbol = substring(token_a_mint from 1 for 4) || '..' || substring(token_a_mint from greatest(char_length(token_a_mint)-3, 1) for 4)
    ) then $2
    else token_a_symbol
  end,
  token_a_decimals = case
    when $3 > 0 and coalesce(token_a_decimals, 0) = 0 then $3
    else token_a_decimals
  end,
  token_a_uri = case
    when $4 <> '' and coalesce(token_a_uri, '') = '' then $4
    else token_a_uri
  end
where token_a_mint = $1
  and (
    ($2 <> '' and (
      coalesce(token_a_symbol, '') = ''
      or token_a_symbol = substring(token_a_mint from 1 for 4) || '..' || substring(token_a_mint from greatest(char_length(token_a_mint)-3, 1) for 4)
    ))
    or ($3 > 0 and coalesce(token_a_decimals, 0) = 0)
    or ($4 <> '' and coalesce(token_a_uri, '') = '')
  )
`;

const updatePositionTokenBSql = `
update positions
set
  token_b_symbol = case
    when $2 <> '' and (
      coalesce(token_b_symbol, '') = ''
      or token_b_symbol = substring(token_b_mint from 1 for 4) || '..' || substring(token_b_mint from greatest(char_length(token_b_mint)-3, 1) for 4)
    ) then $2
    else token_b_symbol
  end,
  token_b_decimals = case
    when $3 > 0 and coalesce(token_b_decimals, 0) = 0 then $3
    else token_b_decimals
  end,
  token_b_uri = case
    when $4 <> '' and coalesce(token_b_uri, '') = '' then $4
    else token_b_uri
  end
where token_b_mint = $1
  and (
    ($2 <> '' and (
      coalesce(token_b_symbol, '') = ''
      or token_b_symbol = substring(token_b_mint from 1 for 4) || '..' || substring(token_b_mint from greatest(char_length(token_b_mint)-3, 1) for 4)
    ))
    or ($3 > 0 and coalesce(token_b_decimals, 0) = 0)
    or ($4 <> '' and coalesce(token_b_uri, '') = '')
  )
`;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function rpcCall(method, params) {
  const payload = {
    jsonrpc: "2.0",
    id: rpcId++,
    method,
    params,
  };

  const response = await fetch(SOLANA_RPC_URL, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error(`RPC ${method} HTTP ${response.status}`);
  }

  const body = await response.json();
  if (body.error) {
    throw new Error(`RPC ${method} failed: ${JSON.stringify(body.error)}`);
  }
  return body.result;
}

async function rpcCallWithRetry(method, params) {
  let delay = RETRY_DELAY_MS;
  let lastError = null;

  for (let attempt = 1; attempt <= MAX_RPC_RETRIES; attempt += 1) {
    try {
      return await rpcCall(method, params);
    } catch (error) {
      lastError = error;
      if (attempt === MAX_RPC_RETRIES) {
        break;
      }
      await sleep(delay);
      delay *= 2;
    }
  }

  throw lastError;
}

function readBorshString(buffer, offset) {
  if (offset + 4 > buffer.length) {
    return null;
  }
  const length = buffer.readUInt32LE(offset);
  const start = offset + 4;
  const end = start + length;
  if (end > buffer.length) {
    return null;
  }
  return {
    value: buffer.subarray(start, end).toString("utf8"),
    offset: end,
  };
}

function sanitizeSymbol(symbol) {
  if (!symbol) {
    return "";
  }
  return symbol
    .replace(/\0/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 32);
}

function sanitizeUri(uri) {
  if (!uri) {
    return "";
  }
  return String(uri).replace(/\0/g, "").trim().slice(0, 512);
}

function decodeMetaplexMetadata(dataBase64) {
  if (!dataBase64) {
    return { symbol: "", uri: "" };
  }
  const buffer = Buffer.from(dataBase64, "base64");

  // key (1) + update_authority (32) + mint (32)
  let offset = 65;
  const name = readBorshString(buffer, offset);
  if (!name) {
    return { symbol: "", uri: "" };
  }
  offset = name.offset;
  const symbol = readBorshString(buffer, offset);
  if (!symbol) {
    return { symbol: "", uri: "" };
  }
  offset = symbol.offset;
  const uri = readBorshString(buffer, offset);
  if (!uri) {
    return { symbol: sanitizeSymbol(symbol.value), uri: "" };
  }

  return {
    symbol: sanitizeSymbol(symbol.value),
    uri: sanitizeUri(uri.value),
  };
}

async function fetchMetadataFromMetaplex(mint) {
  const result = await rpcCallWithRetry("getProgramAccounts", [
    METAPLEX_METADATA_PROGRAM_ID,
    {
      commitment: RPC_COMMITMENT,
      encoding: "base64",
      filters: [{ memcmp: { offset: 33, bytes: mint } }],
    },
  ]);

  if (!Array.isArray(result) || result.length === 0) {
    return "";
  }

  const first = result[0];
  const rawData = first?.account?.data;
  if (!rawData) {
    return { symbol: "", uri: "" };
  }

  const dataBase64 = Array.isArray(rawData) ? rawData[0] : rawData;
  return decodeMetaplexMetadata(dataBase64);
}

async function fetchDecimals(mint) {
  const result = await rpcCallWithRetry("getTokenSupply", [mint, { commitment: RPC_COMMITMENT }]);
  const decimals = Number(result?.value?.decimals || 0);
  if (!Number.isFinite(decimals) || decimals < 0) {
    return 0;
  }
  return decimals;
}

async function resolveMintMetadata(mint) {
  const staticMetadata = STATIC_TOKEN_METADATA[mint] || {
    symbol: "",
    decimals: 0,
    uri: "",
  };
  let symbol = staticMetadata.symbol || "";
  let decimals = staticMetadata.decimals || 0;
  let uri = staticMetadata.uri || "";

  if (decimals <= 0) {
    try {
      decimals = await fetchDecimals(mint);
    } catch (_) {
      decimals = 0;
    }
  }

  if (!symbol) {
    try {
      const metadata = await fetchMetadataFromMetaplex(mint);
      symbol = metadata.symbol || "";
      uri = metadata.uri || "";
    } catch (_) {
      symbol = "";
      uri = "";
    }
  } else if (!uri) {
    try {
      const metadata = await fetchMetadataFromMetaplex(mint);
      uri = metadata.uri || "";
    } catch (_) {
      uri = "";
    }
  }

  return {
    mint,
    symbol: sanitizeSymbol(symbol),
    decimals: Number.isFinite(decimals) ? Math.max(0, Math.floor(decimals)) : 0,
    uri: sanitizeUri(uri),
  };
}

async function runWithConcurrency(items, concurrency, workerFn) {
  const workers = [];
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const current = nextIndex;
      nextIndex += 1;
      if (current >= items.length) {
        return;
      }
      await workerFn(items[current], current);
    }
  }

  const workerCount = Math.min(concurrency, items.length);
  for (let i = 0; i < workerCount; i += 1) {
    workers.push(worker());
  }

  await Promise.all(workers);
}

async function applyMetadata(mint, symbol, decimals, uri) {
  const params = [mint, symbol || "", decimals || 0, uri || ""];

  const poolA = await db.query(updatePoolTokenASql, params);
  const poolB = await db.query(updatePoolTokenBSql, params);
  const positionA = await db.query(updatePositionTokenASql, params);
  const positionB = await db.query(updatePositionTokenBSql, params);

  return (
    poolA.rowCount + poolB.rowCount + positionA.rowCount + positionB.rowCount
  );
}

async function main() {
  const { rows } = await db.query(selectCandidateMintsSql);
  const mints = rows.map((row) => row.mint).filter(Boolean);

  console.error(
    `[metadata-backfill] rpc=${SOLANA_RPC_URL} candidates=${mints.length} concurrency=${BACKFILL_CONCURRENCY} dry_run=${DRY_RUN ? 1 : 0}`,
  );

  if (mints.length === 0) {
    console.error("[metadata-backfill] nothing to backfill");
    return;
  }

  const metadataByMint = new Map();
  let processed = 0;

  await runWithConcurrency(mints, BACKFILL_CONCURRENCY, async (mint) => {
    const metadata = await resolveMintMetadata(mint);
    metadataByMint.set(mint, metadata);
    processed += 1;

    if (processed % 25 === 0 || processed === mints.length) {
      const resolved = Array.from(metadataByMint.values()).filter(
        (item) => item.symbol || item.decimals > 0 || item.uri,
      ).length;
      console.error(
        `[metadata-backfill] progress ${processed}/${mints.length} resolved=${resolved}`,
      );
    }
  });

  const resolvedEntries = Array.from(metadataByMint.values()).filter(
    (item) => item.symbol || item.decimals > 0 || item.uri,
  );

  if (DRY_RUN) {
    console.error(
      `[metadata-backfill] dry-run done resolved=${resolvedEntries.length} unresolved=${mints.length - resolvedEntries.length}`,
    );
    console.error(
      "[metadata-backfill] sample:",
      JSON.stringify(resolvedEntries.slice(0, 10), null, 2),
    );
    return;
  }

  let updatedRows = 0;
  await db.query("begin");
  try {
    for (const entry of resolvedEntries) {
      updatedRows += await applyMetadata(entry.mint, entry.symbol, entry.decimals, entry.uri);
    }
    await db.query("commit");
  } catch (error) {
    await db.query("rollback");
    throw error;
  }

  console.error(
    `[metadata-backfill] completed resolved=${resolvedEntries.length} unresolved=${mints.length - resolvedEntries.length} updated_rows=${updatedRows}`,
  );
}

main()
  .catch((error) => {
    console.error("[metadata-backfill] failed:", error.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await db.end();
  });
