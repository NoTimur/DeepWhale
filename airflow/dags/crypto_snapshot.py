from datetime import datetime, timezone, timedelta

from airflow.sdk import dag, task

@dag(
    dag_id = "crypto_snapshot",
    description = "Snapshot of crypto market",
    schedule = "*/5 * * * *",
    start_date = datetime(2026, 1, 1, tzinfo=timezone.utc),
    end_date = None,
    catchup = False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
        "execution_timeout": timedelta(minutes=2),
    },
    tags = ['crypto']
)
def crypto_snapshot():

    @task
    def extract(**context):
        import requests
        from airflow.sdk import Connection

        api_conn = Connection.get("coingecko_api")

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg = PostgresHook(postgres_conn_id="crypto_postgres")
        coin_ids = [
            row[0]
            for row in pg.get_records("SELECT id FROM coins")
        ]
        if not coin_ids:
            raise ValueError("Table coins empty")

        response = requests.get(
            f"{api_conn.host.rstrip('/')}/coins/markets",
            params={
                "vs_currency": "usd",
                "ids": ",".join(coin_ids),
                "price_change_percentage": "24h,7d,30d",
            },
            headers={
                "x-cg-demo-api-key": api_conn.password,
            },
            timeout=30,
        )
        response.raise_for_status()

        snapshot_at = context["logical_date"].isoformat()
        data = response.json()

        return [
            {
                "snapshot_at": snapshot_at,
                "id": coin["id"],
                "current_price": coin.get("current_price"),
                "market_cap": coin.get("market_cap"),
                "market_cap_rank": coin.get("market_cap_rank"),
                "total_volume": coin.get("total_volume"),
                "high24hr": coin.get("high_24h"),
                "low24hr": coin.get("low_24h"),
                "price_change_pct_24h": coin.get(
                    "price_change_percentage_24h"
                ),
                "price_change_pct_7d": coin.get(
                    "price_change_percentage_7d_in_currency"
                ),
                "price_change_pct_30d": coin.get(
                    "price_change_percentage_30d_in_currency"
                ),
            }
            for coin in data
        ]

    @task
    def transform(data):
        return data

    @task
    def load(data):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        if not data:
            raise ValueError("No data for upload")
        pg = PostgresHook(
            postgres_conn_id = "crypto_postgres"
        )
        columns = [
            "snapshot_at",
            "id",
            "current_price",
            "market_cap",
            "market_cap_rank",
            "total_volume",
            "high24hr",
            "low24hr",
            "price_change_pct_24h",
            "price_change_pct_7d",
            "price_change_pct_30d",
        ]

        rows = [tuple(item[column] for column in columns) for item in data]
        pg.upsert_rows(
            table="coin_snapshot",
            rows=rows,
            target_fields=columns,
            conflict_fields=["id", "snapshot_at"],
            commit_every=250,
        )
        


    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    extracted >> transformed >> loaded

crypto_snapshot()