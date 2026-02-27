#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is required"
  exit 1
fi

INITIAL_BLOCK="${INITIAL_BLOCK:-429978940}"
START_BLOCK="${1:-$INITIAL_BLOCK}"
STOP_BLOCK="${2:-+5000}"
CHUNK_SIZE="${3:-10000}"
LIMIT_PROCESSED_BLOCKS="${4:-}"
SUBSTREAMS_ENDPOINT="${SUBSTREAMS_ENDPOINT:-devnet.sol.streamingfast.io:443}"
BACKFILL_PREPARE_WINDOW="${BACKFILL_PREPARE_WINDOW:-12000}"
REALTIME_CATCHUP_HEADROOM="${REALTIME_CATCHUP_HEADROOM:-300000}"
RETRY_LIMIT_PADDING="${RETRY_LIMIT_PADDING:-50000}"
MAX_LIMIT_RETRIES="${MAX_LIMIT_RETRIES:-8}"

extract_required_limit() {
  local err_file="$1"
  sed -nE 's/.*request needs to process a total of ([0-9]+) blocks.*/\1/p' "$err_file" | tail -n 1
}

run_stream_with_retry() {
  local module_name="$1"
  local start_block="$2"
  local stop_block="$3"
  local final_blocks_only="$4"
  local limit="$5"
  local attempt=1

  while true; do
    local err_file
    err_file="$(mktemp)"

    set +e
    if [[ "$final_blocks_only" == "1" ]]; then
      substreams run ./substreams.yaml "$module_name" \
        -e "$SUBSTREAMS_ENDPOINT" \
        --start-block "$start_block" --final-blocks-only \
        --limit-processed-blocks "$limit" -o jsonl \
        2> >(tee "$err_file" >&2) | \
        DATABASE_URL="$DATABASE_URL" node scripts/ingest.js
    else
      substreams run ./substreams.yaml "$module_name" \
        -e "$SUBSTREAMS_ENDPOINT" \
        --start-block "$start_block" --stop-block "$stop_block" \
        --limit-processed-blocks "$limit" -o jsonl \
        2> >(tee "$err_file" >&2) | \
        DATABASE_URL="$DATABASE_URL" node scripts/ingest.js
    fi
    local substreams_status=${PIPESTATUS[0]}
    local ingest_status=${PIPESTATUS[1]}
    set -e

    if [[ "$substreams_status" -eq 0 && "$ingest_status" -eq 0 ]]; then
      rm -f "$err_file"
      return 0
    fi

    local required_limit
    required_limit="$(extract_required_limit "$err_file")"
    rm -f "$err_file"

    if [[ -n "$required_limit" && "$attempt" -lt "$MAX_LIMIT_RETRIES" ]]; then
      local next_limit=$((required_limit + RETRY_LIMIT_PADDING))
      if [[ "$next_limit" -le "$limit" ]]; then
        next_limit=$((limit + RETRY_LIMIT_PADDING))
      fi
      echo "[retry] ${module_name} limit too small (${limit}), required ${required_limit}, retry with ${next_limit} (attempt ${attempt}/${MAX_LIMIT_RETRIES})"
      limit="$next_limit"
      attempt=$((attempt + 1))
      continue
    fi

    echo "[error] ${module_name} failed (substreams=${substreams_status}, ingest=${ingest_status})."
    return 1
  done
}

echo "Installing deps..."
npm install

echo "Applying schema..."
psql "$DATABASE_URL" -f scripts/schema.sql

if [[ "${RESET_DB:-1}" == "1" ]]; then
  echo "Clearing existing data..."
  psql "$DATABASE_URL" -c "truncate table pool_hourly, positions, pools;"
else
  echo "RESET_DB=0, keeping existing data."
fi

if [[ "$START_BLOCK" -lt "$INITIAL_BLOCK" ]]; then
  echo "START_BLOCK ($START_BLOCK) is before INITIAL_BLOCK ($INITIAL_BLOCK), clamping to INITIAL_BLOCK."
  START_BLOCK="$INITIAL_BLOCK"
fi

if [[ "$STOP_BLOCK" == +* ]]; then
  COUNT=${STOP_BLOCK#+}
  if [[ "$COUNT" -le 0 ]]; then
    echo "Stop block count must be > 0"
    exit 1
  fi
  END_BLOCK=$((START_BLOCK + COUNT - 1))
else
  END_BLOCK=$STOP_BLOCK
fi

current=$START_BLOCK
while [[ $current -le $END_BLOCK ]]; do
  chunk_end=$((current + CHUNK_SIZE - 1))
  if [[ $chunk_end -gt $END_BLOCK ]]; then
    chunk_end=$END_BLOCK
  fi

  chunk_limit="$LIMIT_PROCESSED_BLOCKS"
  if [[ -z "$LIMIT_PROCESSED_BLOCKS" ]]; then
    RANGE=$((chunk_end - current + 1))
    chunk_limit=$((RANGE + BACKFILL_PREPARE_WINDOW))
  fi

  echo "Backfill map_amm_priced from $current to $chunk_end..."
  run_stream_with_retry "map_amm_priced" "$current" "$chunk_end" "0" "$chunk_limit"

  echo "Backfill map_pool_hourly from $current to $chunk_end..."
  run_stream_with_retry "map_pool_hourly" "$current" "$chunk_end" "0" "$chunk_limit"

  current=$((chunk_end + 1))
done

if [[ "${REALTIME:-1}" == "1" ]]; then
  REALTIME_START_BLOCK="${REALTIME_START_BLOCK:-$((END_BLOCK + 1))}"
  if [[ -z "${REALTIME_LIMIT_PROCESSED_BLOCKS:-}" ]]; then
    REALTIME_LIMIT_PROCESSED_BLOCKS=$((CHUNK_SIZE + REALTIME_CATCHUP_HEADROOM))
  fi
  echo "Starting realtime sync from block $REALTIME_START_BLOCK (Ctrl+C to stop)..."
  run_stream_with_retry "map_amm_priced" "$REALTIME_START_BLOCK" "" "1" "$REALTIME_LIMIT_PROCESSED_BLOCKS" &

  PID_AMM=$!

  run_stream_with_retry "map_pool_hourly" "$REALTIME_START_BLOCK" "" "1" "$REALTIME_LIMIT_PROCESSED_BLOCKS" &

  PID_HOURLY=$!

  wait $PID_AMM $PID_HOURLY
else
  echo "Realtime sync disabled (REALTIME=0)."
fi
