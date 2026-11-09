from datetime import datetime, timezone, timedelta

from airflow.sdk import dag, task

@dag(
    dag_id = "stock_snapshot",
    description = "snapshot of stock market",
    schedule = "*/10 * * * *",
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

def stock_snapshot():
    
    @task
    def check_market_open():
        import exchange_calendars as xcals
        import pandas as pd
        from airflow.sdk.exceptions import AirflowSkipException

        calendar = xcals.get_calendar("XNYS")
        now = pd.Timestamp.now(tz="UTC")

        if not calendar.is_open_on_minute(now):
            raise AirflowSkipException(
                f"NYSE is closed at {now.isoformat()}"
            )
    
    @task
    def extract(**context):
        import requests
        from datetime import datetime, timezone
        from airflow.sdk import Connection
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="crypto_postgres")
        symbols = [
            row[0]
            for row in pg.get_records(
                "SELECT symbol FROM stocks WHERE is_active ORDER BY symbol"
            )
        ]
        if not symbols:
            raise ValueError("No active stocks")
        
        api_conn = Connection.get("twelve_data")
        response = requests.get(
            f"{api_conn.host.rstrip('/')}/quote",
            params={"symbol": ",".join(symbols)},
            headers={"Authorization": f"apikey {api_conn.password}"},
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        quotes = [data] if "symbol" in data else data.values()

        snapshot_at = context["logical_date"].isoformat()

        return [
            {
                "snapshot_at": snapshot_at,
                "symbol": quote["symbol"],
                "price": float(quote["close"]),
                "change": float(quote["change"]),
                "percent_change": float(quote["percent_change"]),
                "volume": int(quote["volume"]),
                "is_market_open": quote["is_market_open"],
            }
            for quote in quotes
        ]
    @task
    def transform(quotes):
        return quotes

    @task
    def load(data):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        if not data:
            raise ValueError("No data for upload")

        pg = PostgresHook(postgres_conn_id="crypto_postgres")

        columns = [
            "snapshot_at",
            "symbol",
            "price",
            "change",
            "percent_change",
            "volume",
            "is_market_open",
        ]
        rows = [tuple(item[column] for column in columns) for item in data]
        pg.upsert_rows(
            table="stock_snapshot",
            rows=rows,
            target_fields=columns,
            conflict_fields=["symbol", "snapshot_at"],
            commit_every=250,
        )

    market_open = check_market_open()
    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    market_open >> extracted

stock_snapshot()