mod pb;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::str::FromStr;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use pb::mydata::v1 as mydata;
use pb::sf::solana::r#type::v1::{
    CompiledInstruction, InnerInstruction, Message, TransactionStatusMeta, UiTokenAmount,
};
use pb::sf::substreams::solana::v1::Transactions;
use solana_program::pubkey::Pubkey;
use substreams::pb::substreams::Clock;
use substreams::prelude::*;
use substreams::store::{
    StoreAddFloat64, StoreAppend, StoreGetArray, StoreGetFloat64, StoreGetProto, StoreGetString,
    StoreSetFloat64, StoreSetProto, StoreSetString,
};

const AMM_PROGRAM_ID: &str = "ntV2vADY28AaTj7UjxtAyhX8fRCaSbzUfmvXnHZHFzG";
const METAPLEX_METADATA_PROGRAM_ID: &str = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const DEFAULT_FEE_TIER_BPS: u32 = 15;
const HOURS_IN_DAY: i64 = 24;
const POOL_ID_INDEX_KEY: &str = "pool_ids";
const POSITION_ID_INDEX_KEY: &str = "position_ids";
const TOKEN_SYMBOL_KEY_PREFIX: &str = "token_symbol";
const LOG_PROGRAM_DATA_PREFIX: &str = "Program data: ";

const IX_CREATE_POOL: [u8; 8] = [233, 146, 209, 142, 207, 104, 64, 188];
const IX_DEPOSIT: [u8; 8] = [242, 35, 198, 137, 82, 225, 242, 182];
const IX_SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const IX_UPDATE_FEES: [u8; 8] = [225, 27, 13, 6, 69, 84, 172, 191];
const IX_WITHDRAW: [u8; 8] = [183, 18, 70, 156, 148, 109, 161, 34];

const EVENT_LIQUIDITY: [u8; 8] = [164, 92, 200, 16, 136, 60, 73, 17];
const EVENT_POOL_CREATED: [u8; 8] = [25, 94, 75, 47, 112, 99, 53, 63];
const EVENT_SWAP: [u8; 8] = [64, 198, 205, 232, 38, 8, 113, 226];

struct TokenRegistryEntry {
    mint: &'static str,
    symbol: &'static str,
    decimals: u32,
}

const TOKEN_REGISTRY: &[TokenRegistryEntry] = &[TokenRegistryEntry {
    mint: "So11111111111111111111111111111111111111112",
    symbol: "SOL",
    decimals: 9,
}];

const STABLE_ANCHOR_MINTS_RAW: &str = include_str!("stable_mints.txt");
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Default, Clone)]
struct PoolAccumulator {
    pool_id: String,
    token_a_mint: String,
    token_b_mint: String,
    token_a_symbol: String,
    token_b_symbol: String,
    token_a_decimals: u32,
    token_b_decimals: u32,
    fee_tier_bps: u32,
    volume_delta: f64,
    volume_a_delta: f64,
    volume_b_delta: f64,
    fee_delta: f64,
    fee_a_delta: f64,
    fee_b_delta: f64,
    tvl_estimate: f64,
    token_a_balance: f64,
    token_b_balance: f64,
    has_balance_snapshot: bool,
    lp_mint: String,
    lp_supply: f64,
}

#[derive(Default, Clone)]
struct PositionAccumulator {
    position_id: String,
    pool_id: String,
    owner: String,
    token_a_mint: String,
    token_b_mint: String,
    token_a_symbol: String,
    token_b_symbol: String,
    token_a_decimals: u32,
    token_b_decimals: u32,
    fee_tier_bps: u32,
    pool_value: f64,
    token_a_balance: f64,
    token_b_balance: f64,
    lp_mint: String,
    lp_balance: f64,
    pending_rewards: f64,
}

enum AmmEvent {
    PoolCreated(PoolCreatedEvent),
    Liquidity(LiquidityEvent),
    Swap(SwapEvent),
}

struct PoolCreatedEvent {
    pool_id: String,
    token_0_mint: String,
    token_1_mint: String,
    reserve_0_after: u64,
    reserve_1_after: u64,
    token_0_decimals: u8,
    token_1_decimals: u8,
    lp_mint: String,
}

struct LiquidityEvent {
    pool_id: String,
    token_0_mint: String,
    token_1_mint: String,
    reserve_0_after: u64,
    reserve_1_after: u64,
    lp_mint: String,
    lp_supply_after: u64,
}

struct SwapEvent {
    pool_id: String,
    token_in: String,
    amount_in: u64,
    protocol_fee: u64,
    lp_fee: u64,
    lp_fee_bps: u16,
    protocol_fee_bps: u16,
    reserve_0_after: u64,
    reserve_1_after: u64,
}

struct TokenBalanceInfo {
    mint: String,
    owner: String,
    ui_amount: f64,
}

struct TokenMetadataFields {
    name: String,
    symbol: String,
    uri: String,
}

trait InstructionLike {
    fn program_id_index(&self) -> u32;
    fn accounts(&self) -> &[u8];
    fn data(&self) -> &[u8];
}

impl InstructionLike for CompiledInstruction {
    fn program_id_index(&self) -> u32 {
        self.program_id_index
    }

    fn accounts(&self) -> &[u8] {
        &self.accounts
    }

    fn data(&self) -> &[u8] {
        &self.data
    }
}

impl InstructionLike for InnerInstruction {
    fn program_id_index(&self) -> u32 {
        self.program_id_index
    }

    fn accounts(&self) -> &[u8] {
        &self.accounts
    }

    fn data(&self) -> &[u8] {
        &self.data
    }
}

#[substreams::handlers::map]
fn map_amm_updates(
    clock: Clock,
    transactions: Transactions,
    token_symbols: StoreGetString,
) -> mydata::MyData {
    let mut pool_updates: HashMap<String, PoolAccumulator> = HashMap::new();
    let mut position_updates: HashMap<String, PositionAccumulator> = HashMap::new();
    let mut last_global_fee_tier: Option<u32> = None;

    for tx in transactions.transactions.iter() {
        if tx
            .meta
            .as_ref()
            .and_then(|meta| meta.err.as_ref())
            .is_some()
        {
            continue;
        }

        let message = match tx.transaction.as_ref().and_then(|trx| trx.message.as_ref()) {
            Some(message) => message,
            None => continue,
        };

        let account_keys = resolved_account_keys(message, tx.meta.as_ref());
        let mint_decimals = collect_mint_decimals(tx.meta.as_ref());
        let post_token_balances = collect_post_token_balances(tx.meta.as_ref());
        let mut tx_pool_mints: HashMap<String, (String, String)> = HashMap::new();

        for ix in message.instructions.iter() {
            let program_id = account_keys
                .get(ix.program_id_index as usize)
                .map(|value| value.as_str())
                .unwrap_or_default();
            if program_id != AMM_PROGRAM_ID {
                continue;
            }
            apply_amm_instruction(
                &account_keys,
                &ix.accounts,
                &ix.data,
                &mut tx_pool_mints,
                &mut pool_updates,
                &mut position_updates,
                &mut last_global_fee_tier,
                &mint_decimals,
                &post_token_balances,
                &token_symbols,
            );
        }

        if let Some(meta) = tx.meta.as_ref() {
            for inner in meta.inner_instructions.iter() {
                for ix in inner.instructions.iter() {
                    let program_id = account_keys
                        .get(ix.program_id_index as usize)
                        .map(|value| value.as_str())
                        .unwrap_or_default();
                    if program_id != AMM_PROGRAM_ID {
                        continue;
                    }
                    apply_amm_instruction(
                        &account_keys,
                        &ix.accounts,
                        &ix.data,
                        &mut tx_pool_mints,
                        &mut pool_updates,
                        &mut position_updates,
                        &mut last_global_fee_tier,
                        &mint_decimals,
                        &post_token_balances,
                        &token_symbols,
                    );
                }
            }
        }

        if let Some(meta) = tx.meta.as_ref() {
            for encoded_event in extract_anchor_event_bytes(&meta.log_messages) {
                if let Some(event) = parse_anchor_event(&encoded_event) {
                    apply_amm_event(
                        event,
                        &mint_decimals,
                        &tx_pool_mints,
                        &mut pool_updates,
                        last_global_fee_tier,
                        &token_symbols,
                    );
                }
            }
        }
    }

    let mut lp_supply_fallback: HashMap<String, f64> = HashMap::new();
    for position in position_updates.values() {
        if position.pool_id.is_empty() || position.lp_balance <= 0.0 {
            continue;
        }
        let entry = lp_supply_fallback
            .entry(position.pool_id.clone())
            .or_insert(0.0);
        *entry += position.lp_balance;
    }

    for position in position_updates.values_mut() {
        if let Some(pool) = pool_updates.get(&position.pool_id) {
            if !pool.token_a_mint.is_empty() {
                position.token_a_mint = pool.token_a_mint.clone();
            }
            if !pool.token_b_mint.is_empty() {
                position.token_b_mint = pool.token_b_mint.clone();
            }
            if !pool.token_a_symbol.is_empty() {
                position.token_a_symbol = pool.token_a_symbol.clone();
            }
            if !pool.token_b_symbol.is_empty() {
                position.token_b_symbol = pool.token_b_symbol.clone();
            }
            if pool.token_a_decimals > 0 {
                position.token_a_decimals = pool.token_a_decimals;
            }
            if pool.token_b_decimals > 0 {
                position.token_b_decimals = pool.token_b_decimals;
            }
            if position.fee_tier_bps == 0 {
                position.fee_tier_bps = pool.fee_tier_bps;
            }
            if pool.tvl_estimate > 0.0 {
                position.pool_value = pool.tvl_estimate;
            }
            let mut lp_supply = pool.lp_supply;
            if lp_supply <= 0.0 {
                lp_supply = lp_supply_fallback
                    .get(&position.pool_id)
                    .copied()
                    .unwrap_or(0.0);
            }
            if position.lp_balance > 0.0 && lp_supply > 0.0 {
                let ratio = position.lp_balance / lp_supply;
                position.token_a_balance = pool.token_a_balance * ratio;
                position.token_b_balance = pool.token_b_balance * ratio;
                position.pool_value = pool.tvl_estimate * ratio;
            }
        }
    }

    let mut pools: Vec<mydata::Pool> = pool_updates
        .into_values()
        .map(|value| mydata::Pool {
            pool_id: value.pool_id,
            token_a_mint: value.token_a_mint,
            token_b_mint: value.token_b_mint,
            token_a_symbol: value.token_a_symbol,
            token_b_symbol: value.token_b_symbol,
            token_a_decimals: value.token_a_decimals,
            token_b_decimals: value.token_b_decimals,
            fee_tier_bps: value.fee_tier_bps,
            volume_24h: value.volume_delta,
            fees_24h: value.fee_delta,
            apr_24h: 0.0,
            tvl_estimate: value.tvl_estimate,
            token_a_balance: value.token_a_balance,
            token_b_balance: value.token_b_balance,
            tvl_usd: 0.0,
            token_a_price_usd: 0.0,
            token_b_price_usd: 0.0,
            volume_24h_usd: 0.0,
            fees_24h_usd: 0.0,
            apr_24h_usd: 0.0,
            volume_a_delta: value.volume_a_delta,
            volume_b_delta: value.volume_b_delta,
            fees_a_delta: value.fee_a_delta,
            fees_b_delta: value.fee_b_delta,
            lp_supply: value.lp_supply,
        })
        .collect();
    // Compute USD prices and TVL for the pools updated in this block.
    apply_pricing(&mut pools);
    pools.sort_by(|left, right| left.pool_id.cmp(&right.pool_id));

    let mut positions: Vec<mydata::Position> = position_updates
        .into_values()
        .map(|value| mydata::Position {
            position_id: value.position_id,
            pool_id: value.pool_id,
            owner: value.owner,
            token_a_mint: value.token_a_mint,
            token_b_mint: value.token_b_mint,
            token_a_symbol: value.token_a_symbol,
            token_b_symbol: value.token_b_symbol,
            token_a_decimals: value.token_a_decimals,
            token_b_decimals: value.token_b_decimals,
            fee_tier_bps: value.fee_tier_bps,
            pool_value: value.pool_value,
            token_a_balance: value.token_a_balance,
            token_b_balance: value.token_b_balance,
            pending_rewards: value.pending_rewards,
            lp_balance: value.lp_balance,
        })
        .collect();
    positions.sort_by(|left, right| left.position_id.cmp(&right.position_id));

    mydata::MyData {
        slot: clock.number,
        timestamp: timestamp_seconds(&clock),
        pools,
        positions,
    }
}

#[substreams::handlers::store]
fn store_pool_metadata(clock: Clock, updates: mydata::MyData, output: StoreSetProto<mydata::Pool>) {
    for pool in updates.pools.iter() {
        if pool.pool_id.is_empty() {
            continue;
        }
        let key = pool_metadata_key(&pool.pool_id);
        let mut stored = pool.clone();
        stored.volume_a_delta = 0.0;
        stored.volume_b_delta = 0.0;
        stored.fees_a_delta = 0.0;
        stored.fees_b_delta = 0.0;
        output.set(clock.number, key, &stored);
    }
}

#[substreams::handlers::store]
fn store_pool_ids(clock: Clock, updates: mydata::MyData, output: StoreAppend<String>) {
    for pool in updates.pools.iter() {
        if pool.pool_id.is_empty() {
            continue;
        }
        output.append(clock.number, POOL_ID_INDEX_KEY, pool.pool_id.clone());
    }
}

#[substreams::handlers::store]
fn store_pool_tvl(clock: Clock, updates: mydata::MyData, output: StoreSetFloat64) {
    for pool in updates.pools.iter() {
        if pool.pool_id.is_empty() || pool.tvl_estimate <= 0.0 {
            continue;
        }
        let key = pool_tvl_key(&pool.pool_id);
        output.set(clock.number, key, &pool.tvl_estimate);
    }
}

#[substreams::handlers::store]
fn store_pool_lp_supply(clock: Clock, updates: mydata::MyData, output: StoreSetFloat64) {
    for pool in updates.pools.iter() {
        if pool.pool_id.is_empty() || pool.lp_supply <= 0.0 {
            continue;
        }
        let key = pool_lp_supply_key(&pool.pool_id);
        output.set(clock.number, key, &pool.lp_supply);
    }
}

#[substreams::handlers::store]
fn store_pool_volume_1h(clock: Clock, updates: mydata::MyData, output: StoreAddFloat64) {
    let hour_bucket = hour_bucket(timestamp_seconds(&clock));
    for pool in updates.pools.iter() {
        if pool.pool_id.is_empty() || pool.volume_24h <= 0.0 {
            continue;
        }
        let key = pool_volume_hour_key(&pool.pool_id, hour_bucket);
        output.add(clock.number, key, pool.volume_24h);
    }
}

#[substreams::handlers::store]
fn store_pool_volume_a_1h(clock: Clock, updates: mydata::MyData, output: StoreAddFloat64) {
    let hour_bucket = hour_bucket(timestamp_seconds(&clock));
    for pool in updates.pools.iter() {
        if pool.pool_id.is_empty() || pool.volume_a_delta <= 0.0 {
            continue;
        }
        let key = pool_volume_a_hour_key(&pool.pool_id, hour_bucket);
        output.add(clock.number, key, pool.volume_a_delta);
    }
}

#[substreams::handlers::store]
fn store_pool_volume_b_1h(clock: Clock, updates: mydata::MyData, output: StoreAddFloat64) {
    let hour_bucket = hour_bucket(timestamp_seconds(&clock));
    for pool in updates.pools.iter() {
        if pool.pool_id.is_empty() || pool.volume_b_delta <= 0.0 {
            continue;
        }
        let key = pool_volume_b_hour_key(&pool.pool_id, hour_bucket);
        output.add(clock.number, key, pool.volume_b_delta);
    }
}

#[substreams::handlers::store]
fn store_pool_fees_1h(clock: Clock, updates: mydata::MyData, output: StoreAddFloat64) {
    let hour_bucket = hour_bucket(timestamp_seconds(&clock));
    for pool in updates.pools.iter() {
        if pool.pool_id.is_empty() || pool.fees_24h <= 0.0 {
            continue;
        }
        let key = pool_fees_hour_key(&pool.pool_id, hour_bucket);
        output.add(clock.number, key, pool.fees_24h);
    }
}

#[substreams::handlers::store]
fn store_pool_fees_a_1h(clock: Clock, updates: mydata::MyData, output: StoreAddFloat64) {
    let hour_bucket = hour_bucket(timestamp_seconds(&clock));
    for pool in updates.pools.iter() {
        if pool.pool_id.is_empty() || pool.fees_a_delta <= 0.0 {
            continue;
        }
        let key = pool_fees_a_hour_key(&pool.pool_id, hour_bucket);
        output.add(clock.number, key, pool.fees_a_delta);
    }
}

#[substreams::handlers::store]
fn store_pool_fees_b_1h(clock: Clock, updates: mydata::MyData, output: StoreAddFloat64) {
    let hour_bucket = hour_bucket(timestamp_seconds(&clock));
    for pool in updates.pools.iter() {
        if pool.pool_id.is_empty() || pool.fees_b_delta <= 0.0 {
            continue;
        }
        let key = pool_fees_b_hour_key(&pool.pool_id, hour_bucket);
        output.add(clock.number, key, pool.fees_b_delta);
    }
}

#[substreams::handlers::store]
fn store_position_metadata(
    clock: Clock,
    updates: mydata::MyData,
    output: StoreSetProto<mydata::Position>,
) {
    for position in updates.positions.iter() {
        if position.position_id.is_empty() {
            continue;
        }
        let key = position_metadata_key(&position.position_id);
        output.set(clock.number, key, position);
    }
}

#[substreams::handlers::store]
fn store_position_ids(clock: Clock, updates: mydata::MyData, output: StoreAppend<String>) {
    for position in updates.positions.iter() {
        if position.position_id.is_empty() {
            continue;
        }
        output.append(
            clock.number,
            POSITION_ID_INDEX_KEY,
            position.position_id.clone(),
        );
    }
}

#[substreams::handlers::store]
fn store_pool_position_ids(clock: Clock, updates: mydata::MyData, output: StoreAppend<String>) {
    for position in updates.positions.iter() {
        if position.position_id.is_empty() || position.pool_id.is_empty() {
            continue;
        }
        output.append(
            clock.number,
            &pool_position_ids_key(&position.pool_id),
            position.position_id.clone(),
        );
    }
}

#[substreams::handlers::store]
fn store_token_metadata(
    clock: Clock,
    metaplex_transactions: Transactions,
    token_2022_transactions: Transactions,
    output: StoreSetString,
) {
    collect_metaplex_metadata(clock.number, &metaplex_transactions, &output);
    collect_token_2022_metadata(clock.number, &token_2022_transactions, &output);
}

#[substreams::handlers::map]
fn map_my_data(
    clock: Clock,
    pool_metadata: StoreGetProto<mydata::Pool>,
    pool_ids: StoreGetArray<String>,
    pool_tvl: StoreGetFloat64,
    pool_volume_1h: StoreGetFloat64,
    pool_volume_a_1h: StoreGetFloat64,
    pool_volume_b_1h: StoreGetFloat64,
    pool_fees_1h: StoreGetFloat64,
    pool_fees_a_1h: StoreGetFloat64,
    pool_fees_b_1h: StoreGetFloat64,
    position_metadata: StoreGetProto<mydata::Position>,
    position_ids: StoreGetArray<String>,
    token_symbols: StoreGetString,
) -> mydata::MyData {
    let mut pool_id_set = BTreeSet::new();
    if let Some(ids) = pool_ids.get_last(POOL_ID_INDEX_KEY) {
        for id in ids.into_iter().filter(|value| !value.is_empty()) {
            pool_id_set.insert(id);
        }
    }

    let current_hour = hour_bucket(timestamp_seconds(&clock));
    let mut out_pools = Vec::new();
    let mut pool_values: HashMap<String, f64> = HashMap::new();
    let mut pool_fee_tiers: HashMap<String, u32> = HashMap::new();
    let mut pool_volume_a_24h_map: HashMap<String, f64> = HashMap::new();
    let mut pool_volume_b_24h_map: HashMap<String, f64> = HashMap::new();
    let mut pool_fees_a_24h_map: HashMap<String, f64> = HashMap::new();
    let mut pool_fees_b_24h_map: HashMap<String, f64> = HashMap::new();

    for pool_id in pool_id_set.iter() {
        let key = pool_metadata_key(pool_id);
        let Some(mut pool) = pool_metadata.get_last(key) else {
            continue;
        };

        let volume_24h = rolling_24h_sum(&pool_volume_1h, pool_id, current_hour, true);
        let fees_24h = rolling_24h_sum(&pool_fees_1h, pool_id, current_hour, false);
        let volume_a_24h = rolling_24h_sum_for_key(
            &pool_volume_a_1h,
            pool_id,
            current_hour,
            pool_volume_a_hour_key,
        );
        let volume_b_24h = rolling_24h_sum_for_key(
            &pool_volume_b_1h,
            pool_id,
            current_hour,
            pool_volume_b_hour_key,
        );
        let fees_a_24h =
            rolling_24h_sum_for_key(&pool_fees_a_1h, pool_id, current_hour, pool_fees_a_hour_key);
        let fees_b_24h =
            rolling_24h_sum_for_key(&pool_fees_b_1h, pool_id, current_hour, pool_fees_b_hour_key);
        let mut tvl_value = pool_tvl
            .get_last(pool_tvl_key(pool_id))
            .unwrap_or(pool.tvl_estimate);
        if tvl_value <= 0.0 {
            let fallback = pool.token_a_balance + pool.token_b_balance;
            if fallback > 0.0 {
                tvl_value = fallback;
            }
        }

        pool.volume_24h = volume_24h;
        pool.fees_24h = fees_24h;
        pool.tvl_estimate = tvl_value;
        pool.apr_24h = if tvl_value > 0.0 {
            fees_24h * 365.0 / tvl_value
        } else {
            0.0
        };
        if !pool.token_a_mint.is_empty() {
            pool.token_a_symbol =
                resolve_symbol(&pool.token_a_mint, &pool.token_a_symbol, &token_symbols);
        }
        if !pool.token_b_mint.is_empty() {
            pool.token_b_symbol =
                resolve_symbol(&pool.token_b_mint, &pool.token_b_symbol, &token_symbols);
        }
        if pool.token_a_decimals == 0 {
            if let Some(decimals) = decimals_for_mint(&pool.token_a_mint) {
                pool.token_a_decimals = decimals;
            }
        }
        if pool.token_b_decimals == 0 {
            if let Some(decimals) = decimals_for_mint(&pool.token_b_mint) {
                pool.token_b_decimals = decimals;
            }
        }

        pool_values.insert(pool_id.clone(), tvl_value);
        pool_fee_tiers.insert(pool_id.clone(), pool.fee_tier_bps);
        pool_volume_a_24h_map.insert(pool_id.clone(), volume_a_24h);
        pool_volume_b_24h_map.insert(pool_id.clone(), volume_b_24h);
        pool_fees_a_24h_map.insert(pool_id.clone(), fees_a_24h);
        pool_fees_b_24h_map.insert(pool_id.clone(), fees_b_24h);
        out_pools.push(pool);
    }

    let mut position_id_set = BTreeSet::new();
    if let Some(ids) = position_ids.get_last(POSITION_ID_INDEX_KEY) {
        for id in ids.into_iter().filter(|value| !value.is_empty()) {
            position_id_set.insert(id);
        }
    }

    let mut out_positions = Vec::new();
    for position_id in position_id_set.iter() {
        let key = position_metadata_key(position_id);
        let Some(mut position) = position_metadata.get_last(key) else {
            continue;
        };
        if position.fee_tier_bps == 0 {
            position.fee_tier_bps = pool_fee_tiers.get(&position.pool_id).copied().unwrap_or(0);
        }
        if !position.token_a_mint.is_empty() {
            position.token_a_symbol = resolve_symbol(
                &position.token_a_mint,
                &position.token_a_symbol,
                &token_symbols,
            );
        }
        if !position.token_b_mint.is_empty() {
            position.token_b_symbol = resolve_symbol(
                &position.token_b_mint,
                &position.token_b_symbol,
                &token_symbols,
            );
        }
        if position.token_a_decimals == 0 {
            if let Some(pool) = out_pools
                .iter()
                .find(|pool| pool.pool_id == position.pool_id)
            {
                position.token_a_decimals = pool.token_a_decimals;
                position.token_b_decimals = pool.token_b_decimals;
            }
        }
        position.pool_value = pool_values
            .get(&position.pool_id)
            .copied()
            .unwrap_or(position.pool_value);
        out_positions.push(position);
    }

    let prices_usd = apply_pricing(&mut out_pools);

    for pool in out_pools.iter_mut() {
        let price_a = prices_usd.get(&pool.token_a_mint).copied().unwrap_or(0.0);
        let price_b = prices_usd.get(&pool.token_b_mint).copied().unwrap_or(0.0);
        let volume_a_24h = pool_volume_a_24h_map
            .get(&pool.pool_id)
            .copied()
            .unwrap_or(0.0);
        let volume_b_24h = pool_volume_b_24h_map
            .get(&pool.pool_id)
            .copied()
            .unwrap_or(0.0);
        let fees_a_24h = pool_fees_a_24h_map
            .get(&pool.pool_id)
            .copied()
            .unwrap_or(0.0);
        let fees_b_24h = pool_fees_b_24h_map
            .get(&pool.pool_id)
            .copied()
            .unwrap_or(0.0);

        pool.volume_24h_usd = volume_a_24h * price_a + volume_b_24h * price_b;
        pool.fees_24h_usd = fees_a_24h * price_a + fees_b_24h * price_b;
        pool.apr_24h_usd = if pool.tvl_usd > 0.0 {
            pool.fees_24h_usd * 365.0 / pool.tvl_usd
        } else {
            0.0
        };
    }

    mydata::MyData {
        slot: clock.number,
        timestamp: timestamp_seconds(&clock),
        pools: out_pools,
        positions: out_positions,
    }
}

fn apply_pricing(pools: &mut [mydata::Pool]) -> HashMap<String, f64> {
    let mut prices_usd: HashMap<String, f64> = HashMap::new();
    let mut weights: HashMap<String, f64> = HashMap::new();

    for pool in pools.iter() {
        if is_stable_mint(&pool.token_a_mint) {
            prices_usd.insert(pool.token_a_mint.clone(), 1.0);
        }
        if is_stable_mint(&pool.token_b_mint) {
            prices_usd.insert(pool.token_b_mint.clone(), 1.0);
        }
    }

    let sol_price = derive_sol_price_usd(pools);
    if sol_price > 0.0 {
        for pool in pools.iter() {
            if is_sol_mint(&pool.token_a_mint) {
                prices_usd.insert(pool.token_a_mint.clone(), sol_price);
            }
            if is_sol_mint(&pool.token_b_mint) {
                prices_usd.insert(pool.token_b_mint.clone(), sol_price);
            }
        }
    }

    for pool in pools.iter() {
        if pool.token_a_balance <= 0.0 || pool.token_b_balance <= 0.0 {
            continue;
        }

        let (a_is_stable, b_is_stable) = (
            is_stable_mint(&pool.token_a_mint),
            is_stable_mint(&pool.token_b_mint),
        );
        let (a_is_sol, b_is_sol) = (
            is_sol_mint(&pool.token_a_mint),
            is_sol_mint(&pool.token_b_mint),
        );

        if a_is_stable {
            let price = pool.token_a_balance / pool.token_b_balance;
            update_weighted_price(
                &mut prices_usd,
                &mut weights,
                &pool.token_b_mint,
                price,
                pool.token_a_balance,
            );
            continue;
        }

        if b_is_stable {
            let price = pool.token_b_balance / pool.token_a_balance;
            update_weighted_price(
                &mut prices_usd,
                &mut weights,
                &pool.token_a_mint,
                price,
                pool.token_b_balance,
            );
            continue;
        }

        if a_is_sol && sol_price > 0.0 {
            let price = (pool.token_a_balance * sol_price) / pool.token_b_balance;
            update_weighted_price(
                &mut prices_usd,
                &mut weights,
                &pool.token_b_mint,
                price,
                pool.token_a_balance * sol_price,
            );
            continue;
        }

        if b_is_sol && sol_price > 0.0 {
            let price = (pool.token_b_balance * sol_price) / pool.token_a_balance;
            update_weighted_price(
                &mut prices_usd,
                &mut weights,
                &pool.token_a_mint,
                price,
                pool.token_b_balance * sol_price,
            );
        }
    }

    for pool in pools.iter_mut() {
        let price_a = prices_usd.get(&pool.token_a_mint).copied().unwrap_or(0.0);
        let price_b = prices_usd.get(&pool.token_b_mint).copied().unwrap_or(0.0);
        pool.token_a_price_usd = price_a;
        pool.token_b_price_usd = price_b;

        pool.tvl_usd = pool.token_a_balance * price_a + pool.token_b_balance * price_b;
    }

    prices_usd
}

fn derive_sol_price_usd(pools: &[mydata::Pool]) -> f64 {
    let mut weighted_price = 0.0;
    let mut total_weight = 0.0;
    for pool in pools.iter() {
        if pool.token_a_balance <= 0.0 || pool.token_b_balance <= 0.0 {
            continue;
        }
        let (a_is_sol, b_is_sol) = (
            is_sol_mint(&pool.token_a_mint),
            is_sol_mint(&pool.token_b_mint),
        );
        let (a_is_stable, b_is_stable) = (
            is_stable_mint(&pool.token_a_mint),
            is_stable_mint(&pool.token_b_mint),
        );

        if a_is_sol && b_is_stable {
            let price = pool.token_b_balance / pool.token_a_balance;
            weighted_price += price * pool.token_b_balance;
            total_weight += pool.token_b_balance;
        } else if b_is_sol && a_is_stable {
            let price = pool.token_a_balance / pool.token_b_balance;
            weighted_price += price * pool.token_a_balance;
            total_weight += pool.token_a_balance;
        }
    }

    if total_weight > 0.0 {
        weighted_price / total_weight
    } else {
        0.0
    }
}

fn update_weighted_price(
    prices: &mut HashMap<String, f64>,
    weights: &mut HashMap<String, f64>,
    mint: &str,
    price: f64,
    weight: f64,
) {
    if price <= 0.0 || weight <= 0.0 {
        return;
    }
    let current_price = prices.get(mint).copied().unwrap_or(0.0);
    let current_weight = weights.get(mint).copied().unwrap_or(0.0);
    let new_weight = current_weight + weight;
    let new_price = if current_weight > 0.0 {
        (current_price * current_weight + price * weight) / new_weight
    } else {
        price
    };
    prices.insert(mint.to_string(), new_price);
    weights.insert(mint.to_string(), new_weight);
}

fn is_stable_mint(mint: &str) -> bool {
    STABLE_ANCHOR_MINTS_RAW
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .any(|line| line == mint)
}

fn is_sol_mint(mint: &str) -> bool {
    mint == SOL_MINT
}

#[substreams::handlers::map]
fn map_pool_hourly(clock: Clock, updates: mydata::MyData) -> mydata::HourlyData {
    let hour = hour_bucket(timestamp_seconds(&clock));
    let mut pool_hours = Vec::new();
    for pool in updates.pools.iter() {
        if pool.pool_id.is_empty() {
            continue;
        }
        let volume_a = pool.volume_a_delta;
        let volume_b = pool.volume_b_delta;
        let fees_a = pool.fees_a_delta;
        let fees_b = pool.fees_b_delta;
        if volume_a <= 0.0 && volume_b <= 0.0 && fees_a <= 0.0 && fees_b <= 0.0 {
            continue;
        }
        pool_hours.push(mydata::PoolHour {
            pool_id: pool.pool_id.clone(),
            hour,
            volume_a,
            volume_b,
            fees_a,
            fees_b,
        });
    }

    mydata::HourlyData {
        slot: clock.number,
        timestamp: timestamp_seconds(&clock),
        pool_hours,
    }
}

#[substreams::handlers::map]
fn map_amm_priced(
    clock: Clock,
    updates: mydata::MyData,
    pool_metadata: StoreGetProto<mydata::Pool>,
    pool_ids: StoreGetArray<String>,
    pool_lp_supply: StoreGetFloat64,
    position_metadata: StoreGetProto<mydata::Position>,
    position_ids: StoreGetArray<String>,
    pool_position_ids: StoreGetArray<String>,
    token_symbols: StoreGetString,
) -> mydata::MyData {
    let mut pool_id_set = BTreeSet::new();
    if let Some(ids) = pool_ids.get_last(POOL_ID_INDEX_KEY) {
        for id in ids.into_iter().filter(|value| !value.is_empty()) {
            pool_id_set.insert(id);
        }
    }

    let mut all_pools = Vec::new();
    for pool_id in pool_id_set.iter() {
        let key = pool_metadata_key(pool_id);
        let Some(mut pool) = pool_metadata.get_last(key) else {
            continue;
        };
        if !pool.token_a_mint.is_empty() {
            pool.token_a_symbol =
                resolve_symbol(&pool.token_a_mint, &pool.token_a_symbol, &token_symbols);
        }
        if !pool.token_b_mint.is_empty() {
            pool.token_b_symbol =
                resolve_symbol(&pool.token_b_mint, &pool.token_b_symbol, &token_symbols);
        }
        if pool.token_a_decimals == 0 {
            if let Some(decimals) = decimals_for_mint(&pool.token_a_mint) {
                pool.token_a_decimals = decimals;
            }
        }
        if pool.token_b_decimals == 0 {
            if let Some(decimals) = decimals_for_mint(&pool.token_b_mint) {
                pool.token_b_decimals = decimals;
            }
        }
        all_pools.push(pool);
    }

    let prices_usd = apply_pricing(&mut all_pools);

    let mydata::MyData {
        slot,
        timestamp,
        pools: updated_pools,
        positions: updated_positions,
    } = updates;

    let mut out_pools = Vec::new();
    for update in updated_pools.iter() {
        if update.pool_id.is_empty() {
            continue;
        }
        let key = pool_metadata_key(&update.pool_id);
        let mut pool = pool_metadata
            .get_last(key)
            .unwrap_or_else(|| update.clone());
        let mut normalized_update = update.clone();

        // Some instructions expose token accounts in reversed order compared with
        // event reserves. If this block update order is reversed from stored pool
        // order, normalize so balances land on the correct mint side.
        if !pool.token_a_mint.is_empty()
            && !pool.token_b_mint.is_empty()
            && !normalized_update.token_a_mint.is_empty()
            && !normalized_update.token_b_mint.is_empty()
            && normalized_update.token_a_mint == pool.token_b_mint
            && normalized_update.token_b_mint == pool.token_a_mint
        {
            std::mem::swap(
                &mut normalized_update.token_a_mint,
                &mut normalized_update.token_b_mint,
            );
            std::mem::swap(
                &mut normalized_update.token_a_symbol,
                &mut normalized_update.token_b_symbol,
            );
            std::mem::swap(
                &mut normalized_update.token_a_decimals,
                &mut normalized_update.token_b_decimals,
            );
            std::mem::swap(
                &mut normalized_update.token_a_balance,
                &mut normalized_update.token_b_balance,
            );
            std::mem::swap(
                &mut normalized_update.volume_a_delta,
                &mut normalized_update.volume_b_delta,
            );
            std::mem::swap(
                &mut normalized_update.fees_a_delta,
                &mut normalized_update.fees_b_delta,
            );
        }

        pool.volume_24h = normalized_update.volume_24h;
        pool.fees_24h = normalized_update.fees_24h;
        pool.apr_24h = normalized_update.apr_24h;
        pool.volume_a_delta = normalized_update.volume_a_delta;
        pool.volume_b_delta = normalized_update.volume_b_delta;
        pool.fees_a_delta = normalized_update.fees_a_delta;
        pool.fees_b_delta = normalized_update.fees_b_delta;
        // Guard against instruction-only updates that do not carry reserve snapshots:
        // those updates otherwise default to 0 and can wipe valid balances.
        let has_non_zero_balance_update = normalized_update.token_a_balance > 0.0
            || normalized_update.token_b_balance > 0.0
            || normalized_update.tvl_estimate > 0.0;
        let has_all_zero_balance_update = normalized_update.token_a_balance == 0.0
            && normalized_update.token_b_balance == 0.0
            && normalized_update.tvl_estimate == 0.0;
        let pool_already_zero = pool.token_a_balance == 0.0 && pool.token_b_balance == 0.0;
        let apply_balance_update = has_non_zero_balance_update
            || (has_all_zero_balance_update && pool_already_zero);

        if apply_balance_update {
            if normalized_update.token_a_balance.is_finite()
                && normalized_update.token_a_balance >= 0.0
            {
                pool.token_a_balance = normalized_update.token_a_balance;
            }
            if normalized_update.token_b_balance.is_finite()
                && normalized_update.token_b_balance >= 0.0
            {
                pool.token_b_balance = normalized_update.token_b_balance;
            }
            if normalized_update.tvl_estimate.is_finite() && normalized_update.tvl_estimate >= 0.0
            {
                pool.tvl_estimate = normalized_update.tvl_estimate;
            }
        }

        if !pool.token_a_mint.is_empty() {
            pool.token_a_symbol =
                resolve_symbol(&pool.token_a_mint, &pool.token_a_symbol, &token_symbols);
        }
        if !pool.token_b_mint.is_empty() {
            pool.token_b_symbol =
                resolve_symbol(&pool.token_b_mint, &pool.token_b_symbol, &token_symbols);
        }
        if pool.token_a_decimals == 0 {
            if let Some(decimals) = decimals_for_mint(&pool.token_a_mint) {
                pool.token_a_decimals = decimals;
            }
        }
        if pool.token_b_decimals == 0 {
            if let Some(decimals) = decimals_for_mint(&pool.token_b_mint) {
                pool.token_b_decimals = decimals;
            }
        }

        let price_a = prices_usd.get(&pool.token_a_mint).copied().unwrap_or(0.0);
        let price_b = prices_usd.get(&pool.token_b_mint).copied().unwrap_or(0.0);
        pool.token_a_price_usd = price_a;
        pool.token_b_price_usd = price_b;
        pool.tvl_usd = pool.token_a_balance * price_a + pool.token_b_balance * price_b;

        out_pools.push(pool);
    }

    let out_pools_by_id: HashMap<String, mydata::Pool> = out_pools
        .iter()
        .cloned()
        .map(|pool| (pool.pool_id.clone(), pool))
        .collect();

    let mut out_positions_by_id: HashMap<String, mydata::Position> = HashMap::new();
    for mut position in updated_positions.into_iter() {
        if position.position_id.is_empty() {
            continue;
        }

        if let Some(pool) = out_pools_by_id.get(&position.pool_id) {
            enrich_position_with_pool(&mut position, pool, &token_symbols);
        } else {
            let pool_key = pool_metadata_key(&position.pool_id);
            if let Some(pool) = pool_metadata.get_last(pool_key) {
                enrich_position_with_pool(&mut position, &pool, &token_symbols);
            }
        }

        out_positions_by_id.insert(position.position_id.clone(), position);
    }

    let mut position_ids_by_pool: HashMap<String, HashSet<String>> = HashMap::new();
    if let Some(ids) = position_ids.get_last(POSITION_ID_INDEX_KEY) {
        for position_id in ids.into_iter().filter(|value| !value.is_empty()) {
            if let Some(pool_id) = position_pool_id_from_position_id(&position_id) {
                position_ids_by_pool
                    .entry(pool_id.to_string())
                    .or_default()
                    .insert(position_id);
            }
        }
    }

    // For pools updated this block (e.g. by Swap), emit all positions of that pool with
    // recomputed token_a_balance/token_b_balance.
    for update in updated_pools.iter() {
        let pool_id = update.pool_id.as_str();
        if pool_id.is_empty() {
            continue;
        }

        let Some(pool) = out_pools_by_id.get(pool_id) else {
            continue;
        };

        let mut candidate_position_ids: HashSet<String> = HashSet::new();
        if let Some(ids) = position_ids_by_pool.get(pool_id) {
            for position_id in ids.iter() {
                candidate_position_ids.insert(position_id.clone());
            }
        }
        let key = pool_position_ids_key(pool_id);
        if let Some(ids) = pool_position_ids.get_last(&key) {
            for position_id in ids.into_iter().filter(|value| !value.is_empty()) {
                candidate_position_ids.insert(position_id);
            }
        }
        for position in out_positions_by_id.values() {
            if position.pool_id == pool_id && !position.position_id.is_empty() {
                candidate_position_ids.insert(position.position_id.clone());
            }
        }
        if candidate_position_ids.is_empty() {
            continue;
        }

        let mut positions_for_pool: Vec<mydata::Position> = Vec::new();
        for position_id in candidate_position_ids.iter() {
            let mut position = if let Some(existing) = out_positions_by_id.get(position_id) {
                existing.clone()
            } else {
                let key = position_metadata_key(position_id);
                let Some(position) = position_metadata.get_last(&key) else {
                    continue;
                };
                position
            };

            if position.position_id.is_empty() {
                position.position_id = position_id.clone();
            }
            if position.pool_id.is_empty() {
                position.pool_id = pool_id.to_string();
            }
            if position.pool_id != pool_id {
                continue;
            }

            positions_for_pool.push(position);
        }
        if positions_for_pool.is_empty() {
            continue;
        }

        let mut lp_supply = pool_lp_supply
            .get_last(pool_lp_supply_key(pool_id))
            .unwrap_or(0.0);
        if lp_supply <= 0.0 && pool.lp_supply > 0.0 {
            lp_supply = pool.lp_supply;
        }
        if lp_supply <= 0.0 {
            lp_supply = positions_for_pool
                .iter()
                .filter(|position| position.lp_balance > 0.0)
                .map(|position| position.lp_balance)
                .sum();
        }

        for mut position in positions_for_pool.into_iter() {
            enrich_position_with_pool(&mut position, pool, &token_symbols);
            if position.lp_balance > 0.0 && lp_supply > 0.0 {
                let ratio = position.lp_balance / lp_supply;
                position.token_a_balance = pool.token_a_balance * ratio;
                position.token_b_balance = pool.token_b_balance * ratio;
                position.pool_value = pool.tvl_estimate * ratio;
            }

            out_positions_by_id.insert(position.position_id.clone(), position);
        }
    }

    let mut out_positions: Vec<mydata::Position> = out_positions_by_id.into_values().collect();
    out_positions.sort_by(|a, b| a.position_id.cmp(&b.position_id));

    mydata::MyData {
        slot: if slot > 0 { slot } else { clock.number },
        timestamp: if timestamp > 0 {
            timestamp
        } else {
            timestamp_seconds(&clock)
        },
        pools: out_pools,
        positions: out_positions,
    }
}

fn enrich_position_with_pool(
    position: &mut mydata::Position,
    pool: &mydata::Pool,
    token_symbols: &StoreGetString,
) {
    if !pool.token_a_mint.is_empty() {
        position.token_a_mint = pool.token_a_mint.clone();
    }
    if !pool.token_b_mint.is_empty() {
        position.token_b_mint = pool.token_b_mint.clone();
    }
    if position.fee_tier_bps == 0 && pool.fee_tier_bps > 0 {
        position.fee_tier_bps = pool.fee_tier_bps;
    }
    if !position.token_a_mint.is_empty() {
        position.token_a_symbol = update_symbol_if_needed(
            &position.token_a_mint,
            &position.token_a_symbol,
            token_symbols,
        );
    }
    if !position.token_b_mint.is_empty() {
        position.token_b_symbol = update_symbol_if_needed(
            &position.token_b_mint,
            &position.token_b_symbol,
            token_symbols,
        );
    }
    if position.token_a_decimals == 0 {
        if let Some(decimals) = decimals_for_mint(&position.token_a_mint) {
            position.token_a_decimals = decimals;
        } else if pool.token_a_decimals > 0 {
            position.token_a_decimals = pool.token_a_decimals;
        }
    } else if pool.token_a_decimals > 0 {
        position.token_a_decimals = pool.token_a_decimals;
    }
    if position.token_b_decimals == 0 {
        if let Some(decimals) = decimals_for_mint(&position.token_b_mint) {
            position.token_b_decimals = decimals;
        } else if pool.token_b_decimals > 0 {
            position.token_b_decimals = pool.token_b_decimals;
        }
    } else if pool.token_b_decimals > 0 {
        position.token_b_decimals = pool.token_b_decimals;
    }
}

fn position_pool_id_from_position_id(position_id: &str) -> Option<&str> {
    let (pool_id, _) = position_id.split_once(':')?;
    if pool_id.is_empty() {
        None
    } else {
        Some(pool_id)
    }
}

fn collect_metaplex_metadata(ord: u64, transactions: &Transactions, output: &StoreSetString) {
    for tx in transactions.transactions.iter() {
        if tx
            .meta
            .as_ref()
            .and_then(|meta| meta.err.as_ref())
            .is_some()
        {
            continue;
        }

        let message = match tx.transaction.as_ref().and_then(|trx| trx.message.as_ref()) {
            Some(message) => message,
            None => continue,
        };

        let account_keys = resolved_account_keys(message, tx.meta.as_ref());

        for ix in message.instructions.iter() {
            if let Some((mint, symbol)) = parse_metaplex_metadata_instruction(&account_keys, ix) {
                output.set(ord, token_symbol_key(&mint), &symbol);
            }
        }

        if let Some(meta) = tx.meta.as_ref() {
            for inner in meta.inner_instructions.iter() {
                for ix in inner.instructions.iter() {
                    if let Some((mint, symbol)) =
                        parse_metaplex_metadata_instruction(&account_keys, ix)
                    {
                        output.set(ord, token_symbol_key(&mint), &symbol);
                    }
                }
            }
        }
    }
}

fn collect_token_2022_metadata(ord: u64, transactions: &Transactions, output: &StoreSetString) {
    for tx in transactions.transactions.iter() {
        if tx
            .meta
            .as_ref()
            .and_then(|meta| meta.err.as_ref())
            .is_some()
        {
            continue;
        }

        let message = match tx.transaction.as_ref().and_then(|trx| trx.message.as_ref()) {
            Some(message) => message,
            None => continue,
        };

        let account_keys = resolved_account_keys(message, tx.meta.as_ref());

        for ix in message.instructions.iter() {
            if let Some((mint, symbol)) = parse_token_2022_metadata_instruction(&account_keys, ix) {
                output.set(ord, token_symbol_key(&mint), &symbol);
            }
        }

        if let Some(meta) = tx.meta.as_ref() {
            for inner in meta.inner_instructions.iter() {
                for ix in inner.instructions.iter() {
                    if let Some((mint, symbol)) =
                        parse_token_2022_metadata_instruction(&account_keys, ix)
                    {
                        output.set(ord, token_symbol_key(&mint), &symbol);
                    }
                }
            }
        }
    }
}

fn resolved_account_keys(message: &Message, meta: Option<&TransactionStatusMeta>) -> Vec<String> {
    let mut output: Vec<String> = message
        .account_keys
        .iter()
        .map(|key| bs58::encode(key).into_string())
        .collect();

    if let Some(meta) = meta {
        for key in meta.loaded_writable_addresses.iter() {
            output.push(bs58::encode(key).into_string());
        }
        for key in meta.loaded_readonly_addresses.iter() {
            output.push(bs58::encode(key).into_string());
        }
    }

    output
}

fn parse_metaplex_metadata_instruction(
    account_keys: &[String],
    instruction: &impl InstructionLike,
) -> Option<(String, String)> {
    let program_id = account_keys
        .get(instruction.program_id_index() as usize)
        .map(|value| value.as_str())
        .unwrap_or_default();
    if program_id != METAPLEX_METADATA_PROGRAM_ID {
        return None;
    }

    let metadata_account = resolve_instruction_account(account_keys, instruction.accounts(), 0)?;
    let mint = resolve_instruction_account(account_keys, instruction.accounts(), 1)?;
    if !is_metaplex_metadata_pda(&metadata_account, &mint) {
        return None;
    }

    let meta = parse_metaplex_metadata_fields(instruction.data())?;
    let symbol = sanitize_symbol(&meta.symbol)?;
    Some((mint, symbol))
}

fn parse_token_2022_metadata_instruction(
    account_keys: &[String],
    instruction: &impl InstructionLike,
) -> Option<(String, String)> {
    let program_id = account_keys
        .get(instruction.program_id_index() as usize)
        .map(|value| value.as_str())
        .unwrap_or_default();
    if program_id != TOKEN_2022_PROGRAM_ID {
        return None;
    }

    let mint = resolve_instruction_account(account_keys, instruction.accounts(), 0)?;
    let meta = parse_token_2022_metadata_fields(instruction.data())?;
    let symbol = sanitize_symbol(&meta.symbol)?;
    Some((mint, symbol))
}

fn apply_amm_instruction(
    account_keys: &[String],
    instruction_accounts: &[u8],
    instruction_data: &[u8],
    tx_pool_mints: &mut HashMap<String, (String, String)>,
    pool_updates: &mut HashMap<String, PoolAccumulator>,
    position_updates: &mut HashMap<String, PositionAccumulator>,
    last_global_fee_tier: &mut Option<u32>,
    mint_decimals: &HashMap<String, u8>,
    post_token_balances: &HashMap<u32, TokenBalanceInfo>,
    token_symbols: &StoreGetString,
) {
    let Some(discriminator) = read_discriminator(instruction_data) else {
        return;
    };

    if discriminator == IX_UPDATE_FEES {
        let lp_fee_bps = read_u16_le(instruction_data, 8).unwrap_or(DEFAULT_FEE_TIER_BPS as u16);
        let protocol_fee_bps = read_u16_le(instruction_data, 10).unwrap_or(0);
        *last_global_fee_tier = Some(lp_fee_bps as u32 + protocol_fee_bps as u32);
        return;
    }

    if discriminator == IX_CREATE_POOL {
        let Some(pool_id) = resolve_instruction_account(account_keys, instruction_accounts, 3)
        else {
            return;
        };
        let token_0_mint =
            resolve_instruction_account(account_keys, instruction_accounts, 1).unwrap_or_default();
        let token_1_mint =
            resolve_instruction_account(account_keys, instruction_accounts, 2).unwrap_or_default();
        let lp_mint =
            resolve_instruction_account(account_keys, instruction_accounts, 6).unwrap_or_default();

        tx_pool_mints.insert(
            pool_id.clone(),
            (token_0_mint.clone(), token_1_mint.clone()),
        );
        upsert_pool_metadata(
            pool_updates,
            &pool_id,
            &token_0_mint,
            &token_1_mint,
            *last_global_fee_tier,
            Some(mint_decimals),
            token_symbols,
        );

        let user_lp_token_account =
            resolve_instruction_account(account_keys, instruction_accounts, 9).unwrap_or_default();
        let user_lp_token_account_index =
            resolve_instruction_account_index(instruction_accounts, 9);
        if !user_lp_token_account.is_empty() {
            let position_id = format!("{}:{}", pool_id, user_lp_token_account);
            upsert_position_metadata(
                position_updates,
                &position_id,
                &pool_id,
                &token_0_mint,
                &token_1_mint,
                *last_global_fee_tier,
                Some(mint_decimals),
                token_symbols,
            );
            update_position_lp_balance(
                position_updates,
                &position_id,
                user_lp_token_account_index,
                post_token_balances,
            );
        }
        if !lp_mint.is_empty() {
            if let Some(pool) = pool_updates.get_mut(&pool_id) {
                if pool.lp_mint.is_empty() {
                    pool.lp_mint = lp_mint;
                }
            }
        }
        return;
    }

    if discriminator == IX_DEPOSIT || discriminator == IX_WITHDRAW {
        let Some(pool_id) = resolve_instruction_account(account_keys, instruction_accounts, 1)
        else {
            return;
        };
        let token_0_mint =
            resolve_instruction_account(account_keys, instruction_accounts, 7).unwrap_or_default();
        let token_1_mint =
            resolve_instruction_account(account_keys, instruction_accounts, 8).unwrap_or_default();
        let lp_mint =
            resolve_instruction_account(account_keys, instruction_accounts, 9).unwrap_or_default();

        tx_pool_mints.insert(
            pool_id.clone(),
            (token_0_mint.clone(), token_1_mint.clone()),
        );
        upsert_pool_metadata(
            pool_updates,
            &pool_id,
            &token_0_mint,
            &token_1_mint,
            *last_global_fee_tier,
            Some(mint_decimals),
            token_symbols,
        );

        let user_lp_token_account =
            resolve_instruction_account(account_keys, instruction_accounts, 4).unwrap_or_default();
        let user_lp_token_account_index =
            resolve_instruction_account_index(instruction_accounts, 4);
        if !user_lp_token_account.is_empty() {
            let position_id = format!("{}:{}", pool_id, user_lp_token_account);
            upsert_position_metadata(
                position_updates,
                &position_id,
                &pool_id,
                &token_0_mint,
                &token_1_mint,
                *last_global_fee_tier,
                Some(mint_decimals),
                token_symbols,
            );
            update_position_lp_balance(
                position_updates,
                &position_id,
                user_lp_token_account_index,
                post_token_balances,
            );
        }
        if !lp_mint.is_empty() {
            if let Some(pool) = pool_updates.get_mut(&pool_id) {
                if pool.lp_mint.is_empty() {
                    pool.lp_mint = lp_mint;
                }
            }
        }
        return;
    }

    if discriminator == IX_SWAP {
        let Some(pool_id) = resolve_instruction_account(account_keys, instruction_accounts, 2)
        else {
            return;
        };
        let token_0_mint =
            resolve_instruction_account(account_keys, instruction_accounts, 9).unwrap_or_default();
        let token_1_mint =
            resolve_instruction_account(account_keys, instruction_accounts, 10).unwrap_or_default();

        tx_pool_mints.insert(
            pool_id.clone(),
            (token_0_mint.clone(), token_1_mint.clone()),
        );
        upsert_pool_metadata(
            pool_updates,
            &pool_id,
            &token_0_mint,
            &token_1_mint,
            *last_global_fee_tier,
            Some(mint_decimals),
            token_symbols,
        );
    }
}

fn upsert_pool_metadata(
    pool_updates: &mut HashMap<String, PoolAccumulator>,
    pool_id: &str,
    token_0_mint: &str,
    token_1_mint: &str,
    fee_tier_bps: Option<u32>,
    mint_decimals: Option<&HashMap<String, u8>>,
    token_symbols: &StoreGetString,
) {
    if pool_id.is_empty() {
        return;
    }
    let entry = pool_updates.entry(pool_id.to_string()).or_default();
    if entry.pool_id.is_empty() {
        entry.pool_id = pool_id.to_string();
    }
    if entry.token_a_mint.is_empty() && !token_0_mint.is_empty() {
        entry.token_a_mint = token_0_mint.to_string();
    }
    if entry.token_b_mint.is_empty() && !token_1_mint.is_empty() {
        entry.token_b_mint = token_1_mint.to_string();
    }
    if !token_0_mint.is_empty() {
        entry.token_a_symbol =
            update_symbol_if_needed(token_0_mint, &entry.token_a_symbol, token_symbols);
    }
    if !token_1_mint.is_empty() {
        entry.token_b_symbol =
            update_symbol_if_needed(token_1_mint, &entry.token_b_symbol, token_symbols);
    }
    if entry.token_a_decimals == 0 {
        if let Some(decimals) = decimals_for_mint(token_0_mint) {
            entry.token_a_decimals = decimals;
        } else if let Some(decimals_map) = mint_decimals {
            if let Some(decimals) = decimals_map.get(token_0_mint) {
                entry.token_a_decimals = *decimals as u32;
            }
        }
    }
    if entry.token_b_decimals == 0 {
        if let Some(decimals) = decimals_for_mint(token_1_mint) {
            entry.token_b_decimals = decimals;
        } else if let Some(decimals_map) = mint_decimals {
            if let Some(decimals) = decimals_map.get(token_1_mint) {
                entry.token_b_decimals = *decimals as u32;
            }
        }
    }
    if let Some(value) = fee_tier_bps {
        if value > 0 {
            entry.fee_tier_bps = value;
        }
    }
    if entry.fee_tier_bps == 0 {
        entry.fee_tier_bps = DEFAULT_FEE_TIER_BPS;
    }
}

fn set_pool_balances(entry: &mut PoolAccumulator, balance_a: f64, balance_b: f64) {
    let safe_balance_a = if balance_a.is_finite() && balance_a >= 0.0 {
        balance_a
    } else {
        0.0
    };
    let safe_balance_b = if balance_b.is_finite() && balance_b >= 0.0 {
        balance_b
    } else {
        0.0
    };
    entry.token_a_balance = safe_balance_a;
    entry.token_b_balance = safe_balance_b;
    entry.tvl_estimate = safe_balance_a + safe_balance_b;
    entry.has_balance_snapshot = true;
}

fn update_position_lp_balance(
    position_updates: &mut HashMap<String, PositionAccumulator>,
    position_id: &str,
    account_index: Option<u32>,
    post_token_balances: &HashMap<u32, TokenBalanceInfo>,
) {
    let Some(account_index) = account_index else {
        return;
    };
    let Some(info) = post_token_balances.get(&account_index) else {
        return;
    };
    if let Some(position) = position_updates.get_mut(position_id) {
        if position.lp_mint.is_empty() {
            position.lp_mint = info.mint.clone();
        }
        if position.owner.is_empty() && !info.owner.is_empty() {
            position.owner = info.owner.clone();
        }
        position.lp_balance = info.ui_amount;
    }
}

fn upsert_position_metadata(
    position_updates: &mut HashMap<String, PositionAccumulator>,
    position_id: &str,
    pool_id: &str,
    token_0_mint: &str,
    token_1_mint: &str,
    fee_tier_bps: Option<u32>,
    mint_decimals: Option<&HashMap<String, u8>>,
    token_symbols: &StoreGetString,
) {
    if position_id.is_empty() || pool_id.is_empty() {
        return;
    }
    let entry = position_updates.entry(position_id.to_string()).or_default();
    if entry.position_id.is_empty() {
        entry.position_id = position_id.to_string();
    }
    if entry.pool_id.is_empty() {
        entry.pool_id = pool_id.to_string();
    }
    if entry.token_a_mint.is_empty() && !token_0_mint.is_empty() {
        entry.token_a_mint = token_0_mint.to_string();
    }
    if entry.token_b_mint.is_empty() && !token_1_mint.is_empty() {
        entry.token_b_mint = token_1_mint.to_string();
    }
    if !token_0_mint.is_empty() {
        entry.token_a_symbol =
            update_symbol_if_needed(token_0_mint, &entry.token_a_symbol, token_symbols);
    }
    if !token_1_mint.is_empty() {
        entry.token_b_symbol =
            update_symbol_if_needed(token_1_mint, &entry.token_b_symbol, token_symbols);
    }
    if entry.token_a_decimals == 0 {
        if let Some(decimals) = decimals_for_mint(token_0_mint) {
            entry.token_a_decimals = decimals;
        } else if let Some(decimals_map) = mint_decimals {
            if let Some(decimals) = decimals_map.get(token_0_mint) {
                entry.token_a_decimals = *decimals as u32;
            }
        }
    }
    if entry.token_b_decimals == 0 {
        if let Some(decimals) = decimals_for_mint(token_1_mint) {
            entry.token_b_decimals = decimals;
        } else if let Some(decimals_map) = mint_decimals {
            if let Some(decimals) = decimals_map.get(token_1_mint) {
                entry.token_b_decimals = *decimals as u32;
            }
        }
    }
    if let Some(value) = fee_tier_bps {
        if value > 0 {
            entry.fee_tier_bps = value;
        }
    }
    if entry.fee_tier_bps == 0 {
        entry.fee_tier_bps = DEFAULT_FEE_TIER_BPS;
    }
}

fn apply_amm_event(
    event: AmmEvent,
    mint_decimals: &HashMap<String, u8>,
    tx_pool_mints: &HashMap<String, (String, String)>,
    pool_updates: &mut HashMap<String, PoolAccumulator>,
    last_global_fee_tier: Option<u32>,
    token_symbols: &StoreGetString,
) {
    match event {
        AmmEvent::PoolCreated(value) => {
            upsert_pool_metadata(
                pool_updates,
                &value.pool_id,
                &value.token_0_mint,
                &value.token_1_mint,
                last_global_fee_tier,
                Some(mint_decimals),
                token_symbols,
            );
            let entry = pool_updates.entry(value.pool_id).or_default();
            let balance_a = to_ui_amount(value.reserve_0_after, value.token_0_decimals);
            let balance_b = to_ui_amount(value.reserve_1_after, value.token_1_decimals);
            if !value.token_0_mint.is_empty() {
                entry.token_a_mint = value.token_0_mint.clone();
                entry.token_a_symbol =
                    update_symbol_if_needed(&value.token_0_mint, &entry.token_a_symbol, token_symbols);
            }
            if !value.token_1_mint.is_empty() {
                entry.token_b_mint = value.token_1_mint.clone();
                entry.token_b_symbol =
                    update_symbol_if_needed(&value.token_1_mint, &entry.token_b_symbol, token_symbols);
            }
            entry.token_a_decimals = value.token_0_decimals as u32;
            entry.token_b_decimals = value.token_1_decimals as u32;
            if entry.lp_mint.is_empty() && !value.lp_mint.is_empty() {
                entry.lp_mint = value.lp_mint.clone();
            }
            set_pool_balances(entry, balance_a, balance_b);
        }
        AmmEvent::Liquidity(value) => {
            upsert_pool_metadata(
                pool_updates,
                &value.pool_id,
                &value.token_0_mint,
                &value.token_1_mint,
                last_global_fee_tier,
                Some(mint_decimals),
                token_symbols,
            );
            let token_0_decimals = mint_decimals.get(&value.token_0_mint).copied().unwrap_or(0);
            let token_1_decimals = mint_decimals.get(&value.token_1_mint).copied().unwrap_or(0);
            let entry = pool_updates.entry(value.pool_id).or_default();
            let balance_a = to_ui_amount(value.reserve_0_after, token_0_decimals);
            let balance_b = to_ui_amount(value.reserve_1_after, token_1_decimals);
            if !value.token_0_mint.is_empty() {
                entry.token_a_mint = value.token_0_mint.clone();
                entry.token_a_symbol =
                    update_symbol_if_needed(&value.token_0_mint, &entry.token_a_symbol, token_symbols);
            }
            if !value.token_1_mint.is_empty() {
                entry.token_b_mint = value.token_1_mint.clone();
                entry.token_b_symbol =
                    update_symbol_if_needed(&value.token_1_mint, &entry.token_b_symbol, token_symbols);
            }
            if entry.token_a_decimals == 0 {
                entry.token_a_decimals = token_0_decimals as u32;
            }
            if entry.token_b_decimals == 0 {
                entry.token_b_decimals = token_1_decimals as u32;
            }
            if entry.lp_mint.is_empty() && !value.lp_mint.is_empty() {
                entry.lp_mint = value.lp_mint.clone();
            }
            if entry.lp_mint == value.lp_mint {
                if let Some(decimals) = mint_decimals.get(&value.lp_mint) {
                    entry.lp_supply = to_ui_amount(value.lp_supply_after, *decimals);
                }
            }
            set_pool_balances(entry, balance_a, balance_b);
        }
        AmmEvent::Swap(value) => {
            let (token_0_mint, token_1_mint) =
                pool_token_pair(&value.pool_id, tx_pool_mints, pool_updates);
            upsert_pool_metadata(
                pool_updates,
                &value.pool_id,
                &token_0_mint,
                &token_1_mint,
                last_global_fee_tier,
                Some(mint_decimals),
                token_symbols,
            );

            let token_in_decimals = mint_decimals.get(&value.token_in).copied().unwrap_or(0);
            let volume = to_ui_amount(value.amount_in, token_in_decimals);
            let fees = to_ui_amount(value.protocol_fee + value.lp_fee, token_in_decimals);
            let fee_tier_bps = value.lp_fee_bps as u32 + value.protocol_fee_bps as u32;

            let token_0_decimals = mint_decimals.get(&token_0_mint).copied().unwrap_or(0);
            let token_1_decimals = mint_decimals.get(&token_1_mint).copied().unwrap_or(0);
            let balance_a = to_ui_amount(value.reserve_0_after, token_0_decimals);
            let balance_b = to_ui_amount(value.reserve_1_after, token_1_decimals);

            let entry = pool_updates.entry(value.pool_id).or_default();
            if !token_0_mint.is_empty() {
                entry.token_a_mint = token_0_mint.clone();
                entry.token_a_symbol =
                    update_symbol_if_needed(&token_0_mint, &entry.token_a_symbol, token_symbols);
            }
            if !token_1_mint.is_empty() {
                entry.token_b_mint = token_1_mint.clone();
                entry.token_b_symbol =
                    update_symbol_if_needed(&token_1_mint, &entry.token_b_symbol, token_symbols);
            }
            if token_0_decimals > 0 {
                entry.token_a_decimals = token_0_decimals as u32;
            }
            if token_1_decimals > 0 {
                entry.token_b_decimals = token_1_decimals as u32;
            }
            if volume > 0.0 {
                entry.volume_delta += volume;
            }
            if fees > 0.0 {
                entry.fee_delta += fees;
            }
            if volume > 0.0 || fees > 0.0 {
                if value.token_in == token_0_mint {
                    entry.volume_a_delta += volume;
                    entry.fee_a_delta += fees;
                } else if value.token_in == token_1_mint {
                    entry.volume_b_delta += volume;
                    entry.fee_b_delta += fees;
                }
            }
            if fee_tier_bps > 0 {
                entry.fee_tier_bps = fee_tier_bps;
            }
            set_pool_balances(entry, balance_a, balance_b);
        }
    }
}

fn pool_token_pair(
    pool_id: &str,
    tx_pool_mints: &HashMap<String, (String, String)>,
    pool_updates: &HashMap<String, PoolAccumulator>,
) -> (String, String) {
    // Prefer transaction-local account order. Opposite-direction swaps in the
    // same block can otherwise inherit a stale order from prior transactions.
    if let Some((token_0_mint, token_1_mint)) = tx_pool_mints.get(pool_id) {
        if !token_0_mint.is_empty() || !token_1_mint.is_empty() {
            return (token_0_mint.clone(), token_1_mint.clone());
        }
    }
    if let Some(existing) = pool_updates.get(pool_id) {
        if !existing.token_a_mint.is_empty() || !existing.token_b_mint.is_empty() {
            return (existing.token_a_mint.clone(), existing.token_b_mint.clone());
        }
    }
    tx_pool_mints
        .get(pool_id)
        .cloned()
        .unwrap_or_else(|| (String::new(), String::new()))
}

fn resolve_instruction_account(
    account_keys: &[String],
    instruction_accounts: &[u8],
    account_position: usize,
) -> Option<String> {
    let key_index = *instruction_accounts.get(account_position)? as usize;
    account_keys.get(key_index).cloned()
}

fn resolve_instruction_account_index(
    instruction_accounts: &[u8],
    account_position: usize,
) -> Option<u32> {
    instruction_accounts
        .get(account_position)
        .map(|value| *value as u32)
}

fn read_discriminator(data: &[u8]) -> Option<[u8; 8]> {
    if data.len() < 8 {
        return None;
    }
    data.get(..8)?.try_into().ok()
}

fn read_u16_le(data: &[u8], offset: usize) -> Option<u16> {
    let bytes = data.get(offset..offset + 2)?;
    Some(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn collect_mint_decimals(meta: Option<&TransactionStatusMeta>) -> HashMap<String, u8> {
    let mut output = HashMap::new();
    let Some(meta) = meta else {
        return output;
    };

    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        let decimals = balance
            .ui_token_amount
            .as_ref()
            .map(|value| value.decimals as u8)
            .unwrap_or(0);
        let entry = output.entry(balance.mint.clone()).or_insert(decimals);
        if decimals > *entry {
            *entry = decimals;
        }
    }
    output
}

fn collect_post_token_balances(
    meta: Option<&TransactionStatusMeta>,
) -> HashMap<u32, TokenBalanceInfo> {
    let mut output = HashMap::new();
    let Some(meta) = meta else {
        return output;
    };

    for balance in meta.post_token_balances.iter() {
        let ui_amount = balance
            .ui_token_amount
            .as_ref()
            .map(ui_amount_from_token_balance)
            .unwrap_or(0.0);
        output.insert(
            balance.account_index,
            TokenBalanceInfo {
                mint: balance.mint.clone(),
                owner: balance.owner.clone(),
                ui_amount,
            },
        );
    }

    output
}

fn ui_amount_from_token_balance(amount: &UiTokenAmount) -> f64 {
    if !amount.ui_amount_string.is_empty() {
        if let Ok(value) = amount.ui_amount_string.parse::<f64>() {
            return value;
        }
    }
    if amount.ui_amount != 0.0 {
        return amount.ui_amount;
    }
    if amount.amount.is_empty() {
        return 0.0;
    }
    if let Ok(raw) = amount.amount.parse::<f64>() {
        let scale = 10_f64.powi(amount.decimals as i32);
        if scale > 0.0 {
            return raw / scale;
        }
    }
    0.0
}

fn extract_anchor_event_bytes(log_messages: &[String]) -> Vec<Vec<u8>> {
    let mut output = Vec::new();
    for line in log_messages.iter() {
        if let Some(index) = line.find(LOG_PROGRAM_DATA_PREFIX) {
            let encoded = line
                .get(index + LOG_PROGRAM_DATA_PREFIX.len()..)
                .unwrap_or_default()
                .trim();
            if encoded.is_empty() {
                continue;
            }
            if let Ok(decoded) = B64.decode(encoded) {
                if decoded.len() >= 8 {
                    output.push(decoded);
                }
            }
        }
    }
    output
}

fn parse_anchor_event(data: &[u8]) -> Option<AmmEvent> {
    let discriminator = read_discriminator(data)?;
    let mut reader = BytesReader::new(data.get(8..)?);

    if discriminator == EVENT_POOL_CREATED {
        let pool_id = reader.read_pubkey_string()?;
        reader.read_pubkey_string()?;
        let token_0_mint = reader.read_pubkey_string()?;
        let token_1_mint = reader.read_pubkey_string()?;
        reader.read_pubkey_string()?;
        reader.read_pubkey_string()?;
        reader.read_u64()?;
        reader.read_u64()?;
        reader.read_u64()?;
        let lp_mint = reader.read_pubkey_string()?;
        let reserve_0_after = reader.read_u64()?;
        let reserve_1_after = reader.read_u64()?;
        reader.read_u8()?;
        let token_0_decimals = reader.read_u8()?;
        let token_1_decimals = reader.read_u8()?;
        reader.read_u64()?;
        reader.read_pubkey_string()?;
        reader.read_pubkey_string()?;

        return Some(AmmEvent::PoolCreated(PoolCreatedEvent {
            pool_id,
            token_0_mint,
            token_1_mint,
            reserve_0_after,
            reserve_1_after,
            token_0_decimals,
            token_1_decimals,
            lp_mint,
        }));
    }

    if discriminator == EVENT_LIQUIDITY {
        let pool_id = reader.read_pubkey_string()?;
        reader.read_pubkey_string()?;
        let token_0_mint = reader.read_pubkey_string()?;
        let token_1_mint = reader.read_pubkey_string()?;
        reader.read_u64()?;
        reader.read_u64()?;
        reader.read_u64()?;
        reader.read_bool()?;
        let reserve_0_after = reader.read_u64()?;
        let reserve_1_after = reader.read_u64()?;
        let lp_mint = reader.read_pubkey_string()?;
        let lp_supply_after = reader.read_u64()?;

        return Some(AmmEvent::Liquidity(LiquidityEvent {
            pool_id,
            token_0_mint,
            token_1_mint,
            reserve_0_after,
            reserve_1_after,
            lp_mint,
            lp_supply_after,
        }));
    }

    if discriminator == EVENT_SWAP {
        let pool_id = reader.read_pubkey_string()?;
        reader.read_pubkey_string()?;
        let token_in = reader.read_pubkey_string()?;
        let amount_in = reader.read_u64()?;
        reader.read_u64()?;
        let protocol_fee = reader.read_u64()?;
        let lp_fee = reader.read_u64()?;
        let lp_fee_bps = reader.read_u16()?;
        let protocol_fee_bps = reader.read_u16()?;
        reader.read_u64()?;
        reader.read_u8()?;
        let reserve_0_after = reader.read_u64()?;
        let reserve_1_after = reader.read_u64()?;

        return Some(AmmEvent::Swap(SwapEvent {
            pool_id,
            token_in,
            amount_in,
            protocol_fee,
            lp_fee,
            lp_fee_bps,
            protocol_fee_bps,
            reserve_0_after,
            reserve_1_after,
        }));
    }

    None
}

fn parse_metaplex_metadata_fields(data: &[u8]) -> Option<TokenMetadataFields> {
    if data.len() < 2 {
        return None;
    }

    let mut reader = BorshReader::new(&data[1..]);
    if let Some(meta) = parse_token_data_v2(&mut reader) {
        return Some(meta);
    }

    let mut reader = BorshReader::new(&data[1..]);
    if let Some(meta) = parse_token_data(&mut reader) {
        return Some(meta);
    }

    let mut reader = BorshReader::new(&data[1..]);
    if let Some(meta) = parse_option_token_data_v2(&mut reader) {
        return Some(meta);
    }

    let mut reader = BorshReader::new(&data[1..]);
    parse_option_token_data(&mut reader)
}

fn parse_token_2022_metadata_fields(data: &[u8]) -> Option<TokenMetadataFields> {
    if data.len() < 2 {
        return None;
    }

    let mut reader = BorshReader::new(&data[1..]);
    let name = reader.read_string_max(128)?;
    let symbol = reader.read_string_max(32)?;
    let uri = reader.read_string_max(256)?;
    if reader.remaining() > 0 {
        parse_additional_metadata(&mut reader)?;
    }
    if reader.remaining() != 0 {
        return None;
    }

    Some(TokenMetadataFields { name, symbol, uri })
}

fn parse_additional_metadata(reader: &mut BorshReader) -> Option<()> {
    let count = reader.read_u32()? as usize;
    if count > 32 {
        return None;
    }
    for _ in 0..count {
        reader.read_string_max(64)?;
        reader.read_string_max(256)?;
    }
    Some(())
}

fn parse_token_data(reader: &mut BorshReader) -> Option<TokenMetadataFields> {
    let name = reader.read_string_max(128)?;
    let symbol = reader.read_string_max(32)?;
    let uri = reader.read_string_max(256)?;
    reader.read_u16()?;
    parse_creators(reader)?;
    Some(TokenMetadataFields { name, symbol, uri })
}

fn parse_token_data_v2(reader: &mut BorshReader) -> Option<TokenMetadataFields> {
    let meta = parse_token_data(reader)?;

    match reader.read_u8()? {
        0 => {}
        1 => {
            reader.read_u8()?;
            reader.read_pubkey()?;
        }
        _ => return None,
    }

    match reader.read_u8()? {
        0 => {}
        1 => {
            reader.read_u8()?;
            reader.read_u64()?;
            reader.read_u64()?;
        }
        _ => return None,
    }

    Some(meta)
}

fn parse_option_token_data(reader: &mut BorshReader) -> Option<TokenMetadataFields> {
    match reader.read_u8()? {
        0 => None,
        1 => parse_token_data(reader),
        _ => None,
    }
}

fn parse_option_token_data_v2(reader: &mut BorshReader) -> Option<TokenMetadataFields> {
    match reader.read_u8()? {
        0 => None,
        1 => parse_token_data_v2(reader),
        _ => None,
    }
}

fn parse_creators(reader: &mut BorshReader) -> Option<()> {
    match reader.read_u8()? {
        0 => return Some(()),
        1 => {}
        _ => return None,
    }
    let count = reader.read_u32()? as usize;
    if count > 20 {
        return None;
    }
    for _ in 0..count {
        reader.read_pubkey()?;
        reader.read_u8()?;
        reader.read_u8()?;
    }
    Some(())
}

fn sanitize_symbol(symbol: &str) -> Option<String> {
    let trimmed = symbol
        .trim_matches(|value: char| value == '\0' || value.is_whitespace())
        .to_string();
    if trimmed.is_empty() || trimmed.len() > 32 {
        return None;
    }
    if !trimmed
        .chars()
        .all(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_' | '.'))
    {
        return None;
    }
    Some(trimmed)
}

fn is_metaplex_metadata_pda(metadata_account: &str, mint: &str) -> bool {
    let metadata_key = match Pubkey::from_str(metadata_account) {
        Ok(key) => key,
        Err(_) => return false,
    };
    let mint_key = match Pubkey::from_str(mint) {
        Ok(key) => key,
        Err(_) => return false,
    };
    let program_key = match Pubkey::from_str(METAPLEX_METADATA_PROGRAM_ID) {
        Ok(key) => key,
        Err(_) => return false,
    };
    let seeds: [&[u8]; 3] = [b"metadata", program_key.as_ref(), mint_key.as_ref()];
    let (derived, _) = Pubkey::find_program_address(&seeds, &program_key);
    derived == metadata_key
}

fn to_ui_amount(raw_amount: u64, decimals: u8) -> f64 {
    let scale = 10_f64.powi(decimals as i32);
    if scale <= 0.0 {
        return 0.0;
    }
    raw_amount as f64 / scale
}

fn symbol_for_mint(mint: &str) -> String {
    token_meta_for_mint(mint)
        .map(|meta| meta.symbol.to_string())
        .unwrap_or_default()
}

fn decimals_for_mint(mint: &str) -> Option<u32> {
    token_meta_for_mint(mint).map(|meta| meta.decimals)
}

fn token_meta_for_mint(mint: &str) -> Option<&'static TokenRegistryEntry> {
    TOKEN_REGISTRY.iter().find(|entry| entry.mint == mint)
}

fn resolve_symbol(mint: &str, existing: &str, token_symbols: &StoreGetString) -> String {
    if !existing.is_empty() {
        return existing.to_string();
    }
    let registry = symbol_for_mint(mint);
    if !registry.is_empty() {
        return registry;
    }
    if let Some(symbol) = token_symbols.get_last(token_symbol_key(mint)) {
        if !symbol.is_empty() {
            return symbol;
        }
    }
    fallback_symbol_for_mint(mint)
}

fn update_symbol_if_needed(mint: &str, existing: &str, token_symbols: &StoreGetString) -> String {
    if mint.is_empty() {
        return existing.to_string();
    }
    let fallback = fallback_symbol_for_mint(mint);
    if existing.is_empty() || existing == fallback {
        return resolve_symbol(mint, "", token_symbols);
    }
    existing.to_string()
}

fn fallback_symbol_for_mint(mint: &str) -> String {
    if mint.is_empty() {
        return String::new();
    }
    let length = mint.len();
    if length <= 8 {
        return mint.to_string();
    }
    let prefix = &mint[..4];
    let suffix = &mint[length - 4..];
    format!("{prefix}..{suffix}")
}

fn rolling_24h_sum(
    store: &StoreGetFloat64,
    pool_id: &str,
    current_hour: i64,
    is_volume: bool,
) -> f64 {
    if current_hour < 0 {
        return 0.0;
    }

    let mut total = 0.0;
    for offset in 0..HOURS_IN_DAY {
        let hour = current_hour - offset;
        if hour < 0 {
            break;
        }
        let key = if is_volume {
            pool_volume_hour_key(pool_id, hour)
        } else {
            pool_fees_hour_key(pool_id, hour)
        };
        total += store.get_last(key).unwrap_or(0.0);
    }
    total
}

fn rolling_24h_sum_for_key(
    store: &StoreGetFloat64,
    pool_id: &str,
    current_hour: i64,
    key_fn: fn(&str, i64) -> String,
) -> f64 {
    if current_hour < 0 {
        return 0.0;
    }

    let mut total = 0.0;
    for offset in 0..HOURS_IN_DAY {
        let hour = current_hour - offset;
        if hour < 0 {
            break;
        }
        let key = key_fn(pool_id, hour);
        total += store.get_last(key).unwrap_or(0.0);
    }
    total
}

fn timestamp_seconds(clock: &Clock) -> i64 {
    clock.timestamp.as_ref().map(|ts| ts.seconds).unwrap_or(0)
}

fn hour_bucket(timestamp_seconds: i64) -> i64 {
    if timestamp_seconds <= 0 {
        0
    } else {
        timestamp_seconds / 3600
    }
}

fn pool_metadata_key(pool_id: &str) -> String {
    format!("pool:{pool_id}")
}

fn pool_tvl_key(pool_id: &str) -> String {
    format!("pool_tvl:{pool_id}")
}

fn pool_lp_supply_key(pool_id: &str) -> String {
    format!("pool_lp_supply:{pool_id}")
}

fn pool_position_ids_key(pool_id: &str) -> String {
    format!("pool_positions:{pool_id}")
}

fn pool_volume_hour_key(pool_id: &str, hour: i64) -> String {
    format!("pool_volume:{pool_id}:{hour}")
}

fn pool_fees_hour_key(pool_id: &str, hour: i64) -> String {
    format!("pool_fees:{pool_id}:{hour}")
}

fn pool_volume_a_hour_key(pool_id: &str, hour: i64) -> String {
    format!("pool_volume_a:{pool_id}:{hour}")
}

fn pool_volume_b_hour_key(pool_id: &str, hour: i64) -> String {
    format!("pool_volume_b:{pool_id}:{hour}")
}

fn pool_fees_a_hour_key(pool_id: &str, hour: i64) -> String {
    format!("pool_fees_a:{pool_id}:{hour}")
}

fn pool_fees_b_hour_key(pool_id: &str, hour: i64) -> String {
    format!("pool_fees_b:{pool_id}:{hour}")
}

fn position_metadata_key(position_id: &str) -> String {
    format!("position:{position_id}")
}

fn token_symbol_key(mint: &str) -> String {
    format!("{TOKEN_SYMBOL_KEY_PREFIX}:{mint}")
}

struct BytesReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BytesReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Option<&'a [u8]> {
        let end = self.offset.checked_add(length)?;
        let chunk = self.bytes.get(self.offset..end)?;
        self.offset = end;
        Some(chunk)
    }

    fn read_u8(&mut self) -> Option<u8> {
        Some(*self.read_exact(1)?.first()?)
    }

    fn read_bool(&mut self) -> Option<bool> {
        Some(self.read_u8()? != 0)
    }

    fn read_u16(&mut self) -> Option<u16> {
        let bytes = self.read_exact(2)?;
        Some(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u64(&mut self) -> Option<u64> {
        let bytes = self.read_exact(8)?;
        Some(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_pubkey_string(&mut self) -> Option<String> {
        let key_bytes = self.read_exact(32)?;
        Some(bs58::encode(key_bytes).into_string())
    }
}

struct BorshReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BorshReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_exact(&mut self, length: usize) -> Option<&'a [u8]> {
        let end = self.offset.checked_add(length)?;
        let chunk = self.bytes.get(self.offset..end)?;
        self.offset = end;
        Some(chunk)
    }

    fn read_u8(&mut self) -> Option<u8> {
        Some(*self.read_exact(1)?.first()?)
    }

    fn read_u16(&mut self) -> Option<u16> {
        let bytes = self.read_exact(2)?;
        Some(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Option<u32> {
        let bytes = self.read_exact(4)?;
        Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Option<u64> {
        let bytes = self.read_exact(8)?;
        Some(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_pubkey(&mut self) -> Option<[u8; 32]> {
        let bytes = self.read_exact(32)?;
        bytes.try_into().ok()
    }

    fn read_string_max(&mut self, max_len: usize) -> Option<String> {
        let length = self.read_u32()? as usize;
        if length > max_len {
            return None;
        }
        let bytes = self.read_exact(length)?;
        String::from_utf8(bytes.to_vec()).ok()
    }
}
