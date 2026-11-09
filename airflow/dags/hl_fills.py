from datetime import datetime, timezone, timedelta

from airflow.sdk import dag, task

@dag(
    dag_id = "fills_snapshot",
    description = "snapshot of top wallets fills",
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
def fills_snapshot():
    @task
    def extract():
        import requests
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="crypto_postgres")
        addresses = [
            row[0]
            for row in pg.get_records(
                "SELECT address FROM hl_top_wallets ORDER BY pnl_month DESC LIMIT 5"
            )
        ]
        if not addresses:
            raise ValueError("hl_top_wallets empty")

        now = datetime.now(timezone.utc)
        last_by_address = dict(
            pg.get_records(
                "SELECT address, max(ts) FROM hl_fills GROUP BY address"
            )
        )
        rows = []

        for address in addresses:
            last_ts = last_by_address.get(address)

            if last_ts:
                # минута перекрытия, чтобы не потерять сделки на границе
                start_ms = int(
                    (last_ts - timedelta(seconds=60)).timestamp() * 1000
                )
            else:
                start_ms = int(
                    (now - timedelta(days=1)).timestamp() * 1000
                )

            response = requests.post(
                "https://api.hyperliquid.xyz/info",
                json={
                    "type": "userFillsByTime",
                    "user": address,
                    "startTime": start_ms,
                    "aggregateByTime": True,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            for fill in data:
                rows.append({
                    "tid": int(fill["tid"]),
                    "address": address,
                    "coin": fill["coin"],
                    "dir": fill["dir"],
                    "px": fill["px"],
                    "sz": fill["sz"],
                    "closed_pnl": fill["closedPnl"],
                    "fee": fill["fee"],
                    "ts": datetime.fromtimestamp(
                        fill["time"] / 1000,
                        tz=timezone.utc,
                    ).isoformat(),
                })

        return rows
    
    @task
    def transform(rows):
        return sorted(rows, key=lambda row: row["ts"])
    
    @task
    def load(rows):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not rows:
            print("No fills")
            return

        pg = PostgresHook(postgres_conn_id="crypto_postgres")

        columns = [
            "tid",
            "address",
            "coin",
            "dir",
            "px",
            "sz",
            "closed_pnl",
            "fee",
            "ts",
        ]

        values = [
            tuple(row[column] for column in columns)
            for row in rows
        ]

        pg.upsert_rows(
            table="hl_fills",
            rows=values,
            target_fields=columns,
            conflict_fields=["tid"],
            update_fields=[],
        )


    rows = extract()
    transformed = transform(rows)
    load(transformed)


fills_snapshot()
