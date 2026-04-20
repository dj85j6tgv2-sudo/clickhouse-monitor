import clickhouse_connect


def create_client(config: dict) -> clickhouse_connect.driver.Client:
    ch = config["clickhouse"]
    return clickhouse_connect.get_client(
        host=ch["host"],
        port=ch["port"],
        username=ch["user"],
        password=ch["password"],
        connect_timeout=ch["connect_timeout"],
        query_limit=0,
        send_receive_timeout=ch["query_timeout"],
    )


def check_connection(client: clickhouse_connect.driver.Client) -> bool:
    try:
        result = client.query("SELECT 1")
        return result.result_rows == [(1,)]
    except Exception:
        return False
