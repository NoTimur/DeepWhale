<div align="center">

# DeepWhale

### Follow the money on Hyperliquid.

An open data platform for tracking the most profitable wallets on Hyperliquid — their positions, fills, PnL and long/short behaviour — built for research and following-strategy experiments.

[Русская версия](README.ru.md)

![Python](https://img.shields.io/badge/Python-3.14-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-3.3.2-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-database-4169E1?logo=postgresql&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-observability-6E4AFF?logo=grafana&logoColor=white)
![Status](https://img.shields.io/badge/status-MVP-black)

</div>

---

## Why DeepWhale?

Hyperliquid is one of the few crypto venues where **the entire order flow is public**. Wallets are pseudonymous, but their positions and trades are visible on-chain. That creates a rare opportunity:

- see what the best traders are actually doing;
- measure their performance instead of guessing;
- compare live positioning with price action;
- build and test **following strategies** on real, timestamped data.

DeepWhale turns raw public API responses into a clean, queryable warehouse and an analytics layer.

> This is a research project, not financial advice.

## What you can do

- **Discover** the top wallets by monthly PnL and account value.
- **Track** their open positions: size, direction, entry price, unrealized PnL, leverage, liquidation price.
- **Analyse** their fills: realised PnL, volume, win rate, trade direction.
- **Compare** whale behaviour with market prices from CoinGecko.
- **Research** long/short crowding, wallet concentration and following-strategy returns.

## Architecture

```mermaid
flowchart LR
    HL[Hyperliquid public API] -->|leaderboard, positions, fills| AF[Apache Airflow]
    CG[CoinGecko API] -->|crypto prices| AF
    TD[Twelve Data API] -->|US equities| AF
    AF --> PG[(PostgreSQL)]
    PG --> AN[Analytics / research]
    PG --> GF[Grafana Ops]
    PG -. planned .-> WEB[Web dashboard]
```

Airflow runs the ingestion DAGs. PostgreSQL is the single source of truth. Grafana monitors the pipelines; the user-facing web dashboard is the next layer.

## Pipelines

| DAG | Schedule | Source | Target | Purpose |
|---|---|---|---|---|
| `wallets_snapshot` | daily, `0 0 * * *` | Hyperliquid leaderboard | `hl_top_wallets` | select top 10 wallets by monthly PnL |
| `positions_snapshot` | every 5 min | `clearinghouseState` | `hl_positions` | snapshot current open positions for top 10 wallets |
| `fills_snapshot` | every 5 min | `userFillsByTime` | `hl_fills` | incremental fills for top 5 wallets |
| `crypto_snapshot` | every 5 min | CoinGecko | `coin_snapshot` | BTC / ETH / BNB / GRAM market snapshots |
| `stock_snapshot` | every 10 min | Twelve Data | `stock_snapshot` | US equities with NYSE calendar gate |

Fills are loaded incrementally: the DAG reads `max(ts)` per wallet, subtracts a 60-second overlap and deduplicates by Hyperliquid `tid`. Positions are snapshot-based: each run writes a new state for every open position.

## Observability

A compact Grafana dashboard provides observability for the pipelines: data freshness, DagRun health and basic data quality. It is intentionally separate from the user-facing whale analytics.

| Pipeline health | Data quality |
|---|---|
| ![Grafana dashboard with freshness and DAG health](docs/images/grafana-ops-overview.png) | ![Grafana data quality panels](docs/images/grafana-ops-quality.png) |

## Database schema

### Hyperliquid domain

```mermaid
erDiagram
    hl_top_wallets ||--o{ hl_positions : "address"
    hl_top_wallets ||--o{ hl_fills : "address"

    hl_top_wallets {
        text address PK
        text display_name
        numeric account_value
        numeric pnl_month
        numeric roi_month
        numeric vlm_month
    }

    hl_positions {
        timestamptz snapshot_at PK
        text address PK
        text coin PK
        numeric szi
        numeric entry_px
        numeric unrealized_pnl
        numeric leverage
        numeric liquidation_px
    }

    hl_fills {
        bigint tid PK
        text address
        text coin
        text dir
        numeric px
        numeric sz
        numeric closed_pnl
        numeric fee
        timestamptz ts
    }
```

### Tables

| Table | Grain | Responsibility |
|---|---|---|
| `hl_top_wallets` | one row per wallet | current monthly leaderboard selection: identity, equity, PnL, ROI, volume |
| `hl_positions` | wallet × coin × snapshot | current risk: size, side (from `szi`), entry, uPnL, leverage, liquidation |
| `hl_fills` | one row per fill | immutable trade log: direction, price, size, realised PnL, fee, time |

The model is in **3NF**: no derived `side`, `rank`, `position_value` or `roe` columns — those are calculated in queries. `hl_positions` and `hl_fills` have foreign keys to `hl_top_wallets`.

<details>
<summary><strong>SQL DDL</strong></summary>

```sql
CREATE TABLE hl_top_wallets (
    address        text PRIMARY KEY,
    display_name   text,
    account_value  numeric(30, 6),
    pnl_month      numeric(30, 6),
    roi_month      numeric(20, 10),
    vlm_month      numeric(30, 6)
);

CREATE TABLE hl_positions (
    snapshot_at     timestamptz NOT NULL,
    address         text NOT NULL
                    REFERENCES hl_top_wallets(address) ON DELETE CASCADE,
    coin            text NOT NULL,
    szi             numeric(30, 10),
    entry_px        numeric(30, 10),
    unrealized_pnl  numeric(30, 6),
    leverage        numeric(10, 2),
    liquidation_px  numeric(30, 10),
    PRIMARY KEY (snapshot_at, address, coin)
);

CREATE TABLE hl_fills (
    tid          bigint PRIMARY KEY,
    address      text NOT NULL
                 REFERENCES hl_top_wallets(address) ON DELETE CASCADE,
    coin         text NOT NULL,
    dir          text,
    px           numeric(30, 10),
    sz           numeric(30, 10),
    closed_pnl   numeric(30, 6),
    fee          numeric(30, 6),
    ts           timestamptz NOT NULL
);

CREATE INDEX hl_fills_coin_ts_idx
    ON hl_fills (coin, ts DESC);

CREATE INDEX hl_fills_address_ts_idx
    ON hl_fills (address, ts DESC);

CREATE INDEX hl_positions_coin_snapshot_idx
    ON hl_positions (coin, snapshot_at DESC);
```

</details>

### Market data domain

```mermaid
erDiagram
    coins ||--o{ coin_snapshot : id
    coins ||--o| coin_details : id
    stocks ||--o{ stock_snapshot : symbol

    coins {
        text id PK
        text ticker
        text name
    }

    coin_details {
        text id PK
        text description
        text image_url
        date genesis_date
        jsonb categories
        jsonb links
    }

    coin_snapshot {
        timestamptz snapshot_at PK
        text id PK
        numeric current_price
        numeric market_cap
        integer market_cap_rank
        numeric total_volume
        numeric high24hr
        numeric low24hr
        numeric price_change_pct_24h
        numeric price_change_pct_7d
        numeric price_change_pct_30d
    }

    stocks {
        text symbol PK
        text name
        text exchange
        text mic_code
        text currency
        text instrument_type
        boolean is_active
    }

    stock_snapshot {
        timestamptz snapshot_at PK
        text symbol PK
        numeric price
        numeric change
        numeric percent_change
        bigint volume
        boolean is_market_open
    }
```

| Table | Grain | Responsibility |
|---|---|---|
| `coins` | one row per asset | CoinGecko reference: CoinGecko id, ticker, name |
| `coin_details` | one row per asset | optional metadata: description, image, links, categories |
| `coin_snapshot` | asset × snapshot | price, market cap, rank, volume and 24h/7d/30d changes |
| `stocks` | one row per ticker | US equity reference: symbol, exchange, currency, type |
| `stock_snapshot` | ticker × snapshot | quote price, change, volume and market-open flag |

<details>
<summary><strong>SQL DDL — market data</strong></summary>

```sql
CREATE TABLE coins (
    id      text PRIMARY KEY,
    ticker  text NOT NULL,
    name    text NOT NULL
);

CREATE TABLE coin_details (
    id            text PRIMARY KEY REFERENCES coins(id) ON DELETE CASCADE,
    description   text,
    image_url     text,
    genesis_date  date,
    categories    jsonb,
    links         jsonb
);

CREATE TABLE coin_snapshot (
    snapshot_at           timestamptz NOT NULL,
    id                    text NOT NULL REFERENCES coins(id),
    current_price         numeric,
    market_cap            numeric,
    market_cap_rank       integer,
    total_volume          numeric,
    high24hr              numeric,
    low24hr               numeric,
    price_change_pct_24h  numeric,
    price_change_pct_7d   numeric,
    price_change_pct_30d  numeric,
    PRIMARY KEY (id, snapshot_at)
);

CREATE TABLE stocks (
    symbol           text PRIMARY KEY,
    name             text NOT NULL,
    exchange         text NOT NULL,
    mic_code         text,
    currency         text NOT NULL,
    instrument_type  text NOT NULL DEFAULT 'Common Stock',
    is_active        boolean NOT NULL DEFAULT true,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stock_snapshot (
    snapshot_at     timestamptz NOT NULL,
    symbol          text NOT NULL REFERENCES stocks(symbol),
    price           numeric(18, 6),
    change          numeric(18, 6),
    percent_change  numeric(10, 4),
    volume          bigint,
    is_market_open  boolean,
    PRIMARY KEY (symbol, snapshot_at)
);
```

</details>

## Stack

| Layer | Tools |
|---|---|
| Orchestration | Apache Airflow 3.3.2 |
| Language | Python 3.14 |
| Storage | PostgreSQL |
| Sources | Hyperliquid API, CoinGecko API, Twelve Data API |
| Analytics | SQL, window functions, dimensional modelling |
| Visualisation | Grafana Ops (observability); web dashboard planned |
| Tooling | Git, psql, Airflow UI |

## Quick start

Requirements: Python 3.14, Apache Airflow 3.x with the PostgreSQL provider, and a running PostgreSQL instance.

```bash
git clone https://github.com/NoTimur/DeepWhale.git
cd DeepWhale

python3 -m venv .venv
source .venv/bin/activate

# Create the analytics schema: open psql and paste the DDL from Database schema
psql -h localhost -U crypto_app -d crypto
```

Configure Airflow:

1. Point `dags_folder` at `./airflow/dags`.
2. Create a PostgreSQL connection `crypto_postgres` to the `crypto` database.
3. For the stock DAG, create an HTTP connection `twelve_data` with host `https://api.twelvedata.com` and the API key as password.
4. Hyperliquid needs **no API key**.

Start Airflow:

```bash
airflow standalone
```

Open `http://localhost:8080`, enable the DAGs and trigger `wallets_snapshot` first. The positions and fills DAGs read wallet addresses from `hl_top_wallets`, so the wallet DAG should run before them.

> Secrets are intentionally excluded from the repository. `API.txt`, `ariflow_password.txt` and `.env` are git-ignored.

## Dashboard

> **Coming soon.**

The user-facing web dashboard is the next milestone. Planned views:

- whale leaderboard: PnL, ROI, volume, account value;
- wallet detail: open positions, leverage, liquidation distance, recent fills;
- market view: BTC/ETH price with long/short fill markers.

No web dashboard screenshots are included yet — the section will be updated when the web app is connected.

## Roadmap

- [x] Hyperliquid wallet discovery and monthly ranking
- [x] 3NF PostgreSQL schema
- [x] Incremental fills ingestion with deduplication
- [x] Position snapshots and long/short exposure
- [x] Airflow DAGs for Hyperliquid, CoinGecko and US equities
- [x] Grafana observability dashboard
- [ ] Wallet labels and entity resolution
- [ ] Following-strategy backtester: equal-weight, risk-adjusted, rebalancing rules
- [ ] Alerts for large position changes
- [ ] Pagination for wallets with more than 2,000 fills per window
- [ ] Docker Compose and CI

## Limitations

- The Hyperliquid leaderboard is **opt-in** and pseudonymous. It is not the full universe of wallets.
- `userFillsByTime` returns at most **2,000 fills per response** and only the **10,000 most recent** fills.
- To stay within the **1,200 weight/min** API limit, fills cover the top **5** wallets and positions the top **10**.
- Historical leaderboard snapshots are not provided by the API; the project stores its own snapshots.
- This is a research tool, not a trading system.

## Disclaimer

DeepWhale is an educational and research project. It does not provide investment advice or execute trades. Public on-chain data can be incomplete, delayed or misleading. Past performance of any wallet is not indicative of future results.

