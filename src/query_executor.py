from pathlib import Path
import pandas as pd


def load_sql(sql_dir: Path, domain: str, name: str) -> str:
    sql_file = sql_dir / domain / f"{name}.sql"
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    text = sql_file.read_text()
    lines = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def inject_parameters(
    sql: str,
    cluster: str,
    lookback_hours: int,
    lookback_days: int,
) -> str:
    replacements = {
        "{cluster:String}": f"'{cluster}'",
        "{lookback_hours:UInt32}": str(lookback_hours),
        "{lookback_days:UInt32}": str(lookback_days),
    }
    result = sql
    for placeholder, value in replacements.items():
        result = result.replace(placeholder, value)
    return result


def execute_query(
    client,
    sql_dir: Path,
    domain: str,
    name: str,
    cluster: str,
    lookback_hours: int,
    lookback_days: int,
) -> pd.DataFrame:
    sql = load_sql(sql_dir, domain, name)
    sql = inject_parameters(sql, cluster, lookback_hours, lookback_days)
    try:
        return client.query_df(sql)
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})
