create table if not exists pools (
  pool_id text primary key,
  token_a_mint text,
  token_b_mint text,
  token_a_symbol text,
  token_b_symbol text,
  token_a_uri text,
  token_b_uri text,
  token_a_decimals int,
  token_b_decimals int,
  fee_tier_bps int,
  token_a_balance double precision,
  token_b_balance double precision,
  tvl_estimate double precision,
  tvl_usd double precision,
  token_a_price_usd double precision,
  token_b_price_usd double precision,
  updated_slot bigint
);

create table if not exists positions (
  position_id text primary key,
  pool_id text,
  owner text,
  token_a_mint text,
  token_b_mint text,
  token_a_symbol text,
  token_b_symbol text,
  token_a_uri text,
  token_b_uri text,
  token_a_decimals int,
  token_b_decimals int,
  fee_tier_bps int,
  token_a_balance double precision,
  token_b_balance double precision,
  pool_value double precision,
  updated_slot bigint
);

alter table positions add column if not exists owner text;
alter table pools add column if not exists token_a_uri text;
alter table pools add column if not exists token_b_uri text;
alter table positions add column if not exists token_a_uri text;
alter table positions add column if not exists token_b_uri text;
create index if not exists idx_positions_owner on positions(owner);

create table if not exists pool_hourly (
  pool_id text,
  hour bigint,
  volume_a double precision,
  volume_b double precision,
  fees_a double precision,
  fees_b double precision,
  primary key (pool_id, hour)
);

create index if not exists idx_pool_hourly_pool_hour on pool_hourly(pool_id, hour);

create or replace view pools_24h as
with current_hour as (
  select floor(extract(epoch from now()) / 3600)::bigint as h
),
agg as (
  select
    ph.pool_id,
    sum(ph.volume_a) as volume_a_24h,
    sum(ph.volume_b) as volume_b_24h,
    sum(ph.fees_a) as fees_a_24h,
    sum(ph.fees_b) as fees_b_24h
  from pool_hourly ph, current_hour ch
  where ph.hour between ch.h - 23 and ch.h
  group by ph.pool_id
)
select
  p.*,
  coalesce(a.volume_a_24h, 0) as volume_a_24h,
  coalesce(a.volume_b_24h, 0) as volume_b_24h,
  coalesce(a.fees_a_24h, 0) as fees_a_24h,
  coalesce(a.fees_b_24h, 0) as fees_b_24h,
  (coalesce(a.volume_a_24h,0) * p.token_a_price_usd +
   coalesce(a.volume_b_24h,0) * p.token_b_price_usd) as volume_24h_usd,
  (coalesce(a.fees_a_24h,0) * p.token_a_price_usd +
   coalesce(a.fees_b_24h,0) * p.token_b_price_usd) as fees_24h_usd,
  case when p.tvl_usd > 0
       then (coalesce(a.fees_a_24h,0) * p.token_a_price_usd +
             coalesce(a.fees_b_24h,0) * p.token_b_price_usd) * 365 / p.tvl_usd
       else 0 end as apr_24h_usd
from pools p
left join agg a on a.pool_id = p.pool_id;

create or replace view positions_enriched as
select
  p.position_id,
  p.pool_id,
  p.owner,
  p.token_a_mint,
  p.token_b_mint,
  p.token_a_symbol,
  p.token_b_symbol,
  p.token_a_uri,
  p.token_b_uri,
  p.token_a_decimals,
  p.token_b_decimals,
  p.token_a_balance,
  p.token_b_balance,
  pl.token_a_price_usd,
  pl.token_b_price_usd,
  pl.tvl_usd as pool_tvl_usd,
  (p.token_a_balance * pl.token_a_price_usd +
   p.token_b_balance * pl.token_b_price_usd) as position_value_usd,
  case when pl.tvl_usd > 0
       then (p.token_a_balance * pl.token_a_price_usd +
             p.token_b_balance * pl.token_b_price_usd) / pl.tvl_usd * 100
       else 0 end as position_share_percent,
  p.updated_slot
from positions p
join pools pl on pl.pool_id = p.pool_id;
