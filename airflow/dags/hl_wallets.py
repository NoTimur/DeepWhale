from datetime import datetime, timezone, timedelta

from airflow.sdk import dag, task

@dag(
    dag_id = "wallets_snapshot",
    description = "snapshot of top wallets",
    schedule = "0 0 * * *",
    start_date = datetime(2026, 1, 1, tzinfo=timezone.utc),
    end_date = None,
    catchup = False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
        "execution_timeout": timedelta(minutes=2)
    },
    tags = ['stock']
)
def wallet_snapshot():
    @task
    def extract(limit=10, min_account_value=1_000_000):
        import requests
        rows = requests.get("https://stats-data.hyperliquid.xyz/Mainnet/leaderboard", timeout=60).json()["leaderboardRows"]

        wallets = []

        for row in rows:
            if float(row["accountValue"]) < min_account_value:
                continue

            month = dict(row["windowPerformances"])["month"]

            wallets.append({
                "address": row["ethAddress"],
                "display_name": row.get("displayName"),
                "account_value": float(row["accountValue"]),
                "pnl_month": float(month["pnl"]),
                "roi_month": float(month["roi"]),
                "vlm_month": float(month["vlm"]),
            })

       
        return wallets[:limit]
    
    @task
    def transform(wallets):
        wallets.sort(key=lambda wallet: wallet["pnl_month"], reverse=True)
        return wallets
    
    @task
    def load(wallets):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not wallets:
            raise ValueError("No data")

        pg = PostgresHook(postgres_conn_id="crypto_postgres")

        columns = [
            "address",
            "display_name",
            "account_value",
            "pnl_month",
            "roi_month",
            "vlm_month",
        ]

        rows = [
            tuple(wallet[column] for column in columns)
            for wallet in wallets
        ]

        pg.upsert_rows(
            table="hl_top_wallets",
            rows=rows,
            target_fields=columns,
            conflict_fields=["address"],
        )


    wallets = extract()
    transformed = transform(wallets)
    load(transformed)


wallet_snapshot()
