from datetime import datetime, timezone, timedelta

from airflow.sdk import dag, task

@dag(
    dag_id = "positions_snapshot",
    description = "snapshot of top wallets positions",
    schedule = "*/5 * * * *",
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
def postions_snapshot():
    @task
    def extract():
        import requests
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="crypto_postgres")
        addresses = [
            row[0]
            for row in pg.get_records(
                "SELECT address FROM hl_top_wallets ORDER BY pnl_month DESC LIMIT 10"
            )
        ]
        if not addresses:
            raise ValueError("hl_top_wallets empty")

        snapshot_at = datetime.now(timezone.utc).isoformat()
        rows = []

        for address in addresses:
            response = requests.post(
                "https://api.hyperliquid.xyz/info",
                json={"type": "clearinghouseState", "user": address},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            for item in data["assetPositions"]:
                position = item["position"]
                rows.append({
                    "snapshot_at": snapshot_at,
                    "address": address,
                    "coin": position["coin"],
                    "szi": position["szi"],
                    "entry_px": position["entryPx"],
                    "unrealized_pnl": position["unrealizedPnl"],
                    "leverage": position["leverage"]["value"],
                    "liquidation_px": position["liquidationPx"],
                })

        return rows
    
    @task
    def transform(rows):
        return sorted(rows, key=lambda row: (row["address"], row["coin"]))
    
    @task
    def load(rows):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not rows:
            print("No positions")
            return

        pg = PostgresHook(postgres_conn_id="crypto_postgres")

        columns = [
            "snapshot_at",
            "address",
            "coin",
            "szi",
            "entry_px",
            "unrealized_pnl",
            "leverage",
            "liquidation_px",
        ]

        values = [
            tuple(row[column] for column in columns)
            for row in rows
        ]

        pg.upsert_rows(
            table="hl_positions",
            rows=values,
            target_fields=columns,
            conflict_fields=["snapshot_at", "address", "coin"],
        )


    rows = extract()
    transformed = transform(rows)
    load(transformed)


postions_snapshot()
