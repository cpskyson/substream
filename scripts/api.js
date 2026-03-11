#!/usr/bin/env node
const http = require("http");
const { Pool } = require("pg");

const DATABASE_URL = process.env.DATABASE_URL;
const PORT = Number(process.env.PORT || 3001);
const HOST = process.env.HOST || "0.0.0.0";
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";
const CORS_ALLOW_CREDENTIALS = process.env.CORS_ALLOW_CREDENTIALS === "1";
const SOLANA_CLUSTER = (process.env.SOLANA_CLUSTER || "devnet").trim();
const TOKEN_URL_SOURCE = (process.env.TOKEN_URL_SOURCE || "metadata")
  .trim()
  .toLowerCase();
const TOKEN_EXPLORER_BASE_URL = (
  process.env.TOKEN_EXPLORER_BASE_URL || "https://solscan.io/token"
).trim();

if (!DATABASE_URL) {
  console.error("DATABASE_URL is required");
  process.exit(1);
}

const db = new Pool({
  connectionString: DATABASE_URL,
});

const corsOriginList = String(CORS_ORIGIN)
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);

function resolveAllowedOrigin(requestOrigin) {
  if (corsOriginList.includes("*")) {
    return "*";
  }
  if (!requestOrigin) {
    return "";
  }
  return corsOriginList.includes(requestOrigin) ? requestOrigin : "";
}

function responseHeaders(requestOrigin, requestHeaders) {
  const allowedOrigin = resolveAllowedOrigin(requestOrigin);
  const headers = {
    "content-type": "application/json; charset=utf-8",
    "access-control-allow-methods": "GET,OPTIONS",
    "access-control-allow-headers":
      requestHeaders || "content-type,authorization",
  };

  if (allowedOrigin) {
    headers["access-control-allow-origin"] = allowedOrigin;
    if (allowedOrigin !== "*") {
      headers.vary = "Origin";
    }
    if (CORS_ALLOW_CREDENTIALS) {
      headers["access-control-allow-credentials"] = "true";
    }
  }

  return headers;
}

function sendJson(res, statusCode, payload, requestOrigin, requestHeaders) {
  res.writeHead(statusCode, responseHeaders(requestOrigin, requestHeaders));
  res.end(JSON.stringify(payload));
}

function parseIntInRange(value, fallback, min, max) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  if (parsed < min) return min;
  if (parsed > max) return max;
  return parsed;
}

function buildTokenUrl(mint) {
  if (!mint) return "";
  const base = TOKEN_EXPLORER_BASE_URL.replace(/\/+$/, "");
  const encodedMint = encodeURIComponent(mint);
  const encodedCluster = encodeURIComponent(SOLANA_CLUSTER);
  return `${base}/${encodedMint}?cluster=${encodedCluster}`;
}

function normalizeMetadataUrl(value) {
  if (value === null || value === undefined) return "";
  const url = String(value).trim();
  if (!url) return "";
  return url;
}

function resolveTokenUrl(row, mintKey, uriKey) {
  const metadataUrl = normalizeMetadataUrl(row[uriKey]);
  if (TOKEN_URL_SOURCE !== "explorer") {
    return metadataUrl;
  }
  if (metadataUrl) {
    return metadataUrl;
  }
  return buildTokenUrl(row[mintKey]);
}

function addTokenUrls(rows) {
  return rows.map((row) => ({
    ...row,
    token_a_url: resolveTokenUrl(row, "token_a_mint", "token_a_uri"),
    token_b_url: resolveTokenUrl(row, "token_b_mint", "token_b_uri"),
  }));
}

function orderClause(sort, direction) {
  const fieldMap = {
    volume_24h_usd: "volume_24h_usd",
    tvl_usd: "tvl_usd",
    fees_24h_usd: "fees_24h_usd",
    apr_24h_usd: "apr_24h_usd",
    updated_slot: "updated_slot",
  };
  const field = fieldMap[sort] || "volume_24h_usd";
  const dir = direction === "asc" ? "asc" : "desc";
  return `"${field}" ${dir} nulls last, "pool_id" asc`;
}

function positionsOrderClause(sort, direction) {
  const fieldMap = {
    updated_slot: "updated_slot",
    position_value_usd: "position_value_usd",
    position_share_percent: "position_share_percent",
    pool_tvl_usd: "pool_tvl_usd",
    token_a_balance: "token_a_balance",
    token_b_balance: "token_b_balance",
  };
  const field = fieldMap[sort] || "updated_slot";
  const dir = direction === "asc" ? "asc" : "desc";
  return `"${field}" ${dir} nulls last, "position_id" asc`;
}

async function handleHealth(res, requestOrigin, requestHeaders) {
  try {
    await db.query("select 1");
    sendJson(res, 200, { ok: true }, requestOrigin, requestHeaders);
  } catch (error) {
    sendJson(
      res,
      500,
      { ok: false, error: error.message },
      requestOrigin,
      requestHeaders
    );
  }
}

async function handlePools24h(url, res, requestOrigin, requestHeaders) {
  const rawLimit = url.searchParams.get("limit");
  const allRows = rawLimit === "all";
  const limit = parseIntInRange(rawLimit, 1000, 1, 10000);
  const offset = parseIntInRange(url.searchParams.get("offset"), 0, 0, 1000000);
  const sort = url.searchParams.get("sort");
  const direction = url.searchParams.get("direction");
  const orderBy = orderClause(sort, direction);

  try {
    const countResult = await db.query("select count(*)::bigint as total from pools_24h");
    const first = countResult.rows && countResult.rows[0] ? countResult.rows[0] : null;
    const total = Number((first && first.total) || 0);

    let result;
    if (allRows) {
      const sql = `
        select *
        from pools_24h
        order by ${orderBy}
        offset $1
      `;
      result = await db.query(sql, [offset]);
    } else {
      const sql = `
        select *
        from pools_24h
        order by ${orderBy}
        limit $1 offset $2
      `;
      result = await db.query(sql, [limit, offset]);
    }

    const data = addTokenUrls(result.rows);
    sendJson(res, 200, {
      data,
      pagination: {
        total,
        limit: allRows ? "all" : limit,
        offset,
        returned: data.length,
      },
    }, requestOrigin, requestHeaders);
  } catch (error) {
    sendJson(res, 500, { error: error.message }, requestOrigin, requestHeaders);
  }
}

async function handlePositionsEnriched(url, res, requestOrigin, requestHeaders) {
  const owner = (url.searchParams.get("owner") || "").trim();
  if (!owner) {
    sendJson(
      res,
      400,
      { error: "owner is required" },
      requestOrigin,
      requestHeaders
    );
    return;
  }

  const rawLimit = url.searchParams.get("limit");
  const allRows = rawLimit === "all";
  const limit = parseIntInRange(rawLimit, 1000, 1, 10000);
  const offset = parseIntInRange(url.searchParams.get("offset"), 0, 0, 1000000);
  const sort = url.searchParams.get("sort");
  const direction = url.searchParams.get("direction");
  const orderBy = positionsOrderClause(sort, direction);

  try {
    const countResult = await db.query(
      "select count(*)::bigint as total from positions_enriched where owner = $1",
      [owner]
    );
    const first = countResult.rows && countResult.rows[0] ? countResult.rows[0] : null;
    const total = Number((first && first.total) || 0);

    let result;
    if (allRows) {
      const sql = `
        select *
        from positions_enriched
        where owner = $1
        order by ${orderBy}
        offset $2
      `;
      result = await db.query(sql, [owner, offset]);
    } else {
      const sql = `
        select *
        from positions_enriched
        where owner = $1
        order by ${orderBy}
        limit $2 offset $3
      `;
      result = await db.query(sql, [owner, limit, offset]);
    }

    const data = addTokenUrls(result.rows);
    sendJson(res, 200, {
      data,
      pagination: {
        total,
        limit: allRows ? "all" : limit,
        offset,
        returned: data.length,
      },
    }, requestOrigin, requestHeaders);
  } catch (error) {
    sendJson(res, 500, { error: error.message }, requestOrigin, requestHeaders);
  }
}

const server = http.createServer(async (req, res) => {
  const requestOrigin = req.headers.origin || "";
  const requestHeaders = req.headers["access-control-request-headers"] || "";

  if (req.method === "OPTIONS") {
    res.writeHead(204, responseHeaders(requestOrigin, requestHeaders));
    res.end();
    return;
  }

  const host = req.headers.host || `127.0.0.1:${PORT}`;
  const url = new URL(req.url || "/", `http://${host}`);

  if (req.method === "GET" && url.pathname === "/health") {
    await handleHealth(res, requestOrigin, requestHeaders);
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/pools_24h") {
    await handlePools24h(url, res, requestOrigin, requestHeaders);
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/positions_enriched") {
    await handlePositionsEnriched(url, res, requestOrigin, requestHeaders);
    return;
  }

  sendJson(res, 404, { error: "Not found" }, requestOrigin, requestHeaders);
});

server.listen(PORT, HOST, () => {
  console.log(`[api] listening on http://${HOST}:${PORT}`);
});

async function shutdown() {
  server.close(async () => {
    await db.end();
    process.exit(0);
  });
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
