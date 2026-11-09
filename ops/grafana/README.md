# DeepWhale · Ops dashboard

Importable Grafana dashboard for the DeepWhale ingestion pipelines.

The dashboard is designed for **operations**, not for the user-facing whale analytics. It answers three questions:

1. Is the data fresh?
2. Are the Airflow DAGs healthy?
3. Is the data complete and free of obvious quality issues?

## Files

- `deepwhale-ops.json` — complete Grafana dashboard, 16 panels, no manual panel creation needed.
- `README.md` — this file.

## Before importing

You need two PostgreSQL data sources in Grafana. If they already exist, skip to **Import**.

Open **Connections → Data sources → Add data source → PostgreSQL**.

### `airflow-db`

| Field | Value |
|---|---|
| Host | `localhost:5432` |
| Database | `airflow` |
| User | `airflow_user` |
| Password | `airflow_user` |
| SSL mode | `disable` |

### `crypto-db`

| Field | Value |
|---|---|
| Host | `localhost:5432` |
| Database | `crypto` |
| User | `crypto_app` |
| Password | `crypto_app` |
| SSL mode | `disable` |

Click **Save & test** for both.

## Import

1. Open Grafana at `http://localhost:3000`.
2. In the left menu click **Dashboards**.
3. Click **New** (top right) → **Import**.
4. Click **Upload JSON file** and select `ops/grafana/deepwhale-ops.json`.
   You can also paste the JSON into the text area.
5. On the import screen Grafana asks for the two data source inputs:
   - **Airflow metadata DB** → select `airflow-db`;
   - **DeepWhale crypto DB** → select `crypto-db`.
6. Click **Import**.
7. Open the dashboard **DeepWhale · Ops**.

The `DAG` variable is already configured; it loads values from `airflow.dag_run`.

## What is inside

| Row | Panels | Question |
|---|---|---|
| Traffic lights | failed runs, crypto lag, fills lag, positions lag | Is anything red? |
| Freshness | dataset freshness, wallet-list freshness | Which data is stale? |
| Run health | success rate, runs by state, p95 duration, last runs | Which DAG is unhealthy? |
| Data quality | rows per coin snapshot, missing snapshots, nulls/duplicate tids | Is the data complete? |
| Context | tracked wallets, positions/fills in 24h | Is the volume normal? |
| Runbook | links to Airflow UI and DAGs | Where do I go next? |

## First checks after import

- Set time range to **Last 24 hours** and refresh to **1m** (already set in the dashboard).
- Make sure the `DAG` variable is set to **All**.
- If a panel says **No data**, open the panel menu → **Inspect → Query** and check:
  - the correct data source is selected;
  - the SQL runs in `psql`;
  - the dashboard time range covers the data.
- `stock_snapshot` is expected to have a large lag outside US market hours. Do not alert on it.
- `hl_top_wallets` has no update timestamp; wallet-list freshness is measured by the last successful `wallets_snapshot` DagRun.

## Alerts

The dashboard does not create alert rules automatically. Recommended first rules:

- crypto freshness > 15 minutes;
- fills freshness > 20 minutes;
- failed DagRuns ≥ 2 in 15 minutes;
- `wallets_snapshot` last success > 26 hours.

Create them in **Alerting → Alert rules → New alert rule**. Use the same SQL as the panel and attach a Telegram or email contact point.

## Updating

Edit the dashboard in Grafana, then export it:

**Dashboard settings → JSON model → Copy to clipboard**

Replace `deepwhale-ops.json` with the exported model so the version in the repository stays the source of truth.
