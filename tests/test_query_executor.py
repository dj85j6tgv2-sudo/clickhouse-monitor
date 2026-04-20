import pytest
from pathlib import Path
from src.query_executor import load_sql, inject_parameters


def test_load_sql_reads_file(tmp_path):
    sql_dir = tmp_path / "sql" / "cluster"
    sql_dir.mkdir(parents=True)
    (sql_dir / "node_status.sql").write_text("SELECT hostName() FROM system.one;")
    result = load_sql(tmp_path / "sql", "cluster", "node_status")
    assert result == "SELECT hostName() FROM system.one;"


def test_load_sql_strips_comments(tmp_path):
    sql_dir = tmp_path / "sql" / "disk"
    sql_dir.mkdir(parents=True)
    (sql_dir / "free_space.sql").write_text(
        "-- free_space.sql\n"
        "-- Disk space per disk.\n"
        "SELECT name FROM system.disks;\n"
        "    -- ALERT: something\n"
        "    -- ACTION: do something\n"
    )
    result = load_sql(tmp_path / "sql", "disk", "free_space")
    assert "-- free_space.sql" not in result
    assert "-- ALERT:" not in result
    assert "SELECT name FROM system.disks;" in result


def test_load_sql_missing_file(tmp_path):
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir(parents=True)
    with pytest.raises(FileNotFoundError):
        load_sql(sql_dir, "cluster", "nonexistent")


def test_inject_parameters_cluster():
    sql = "SELECT * FROM clusterAllReplicas({cluster:String}, system.one)"
    result = inject_parameters(sql, cluster="my_cluster", lookback_hours=6, lookback_days=1)
    assert result == "SELECT * FROM clusterAllReplicas('my_cluster', system.one)"


def test_inject_parameters_lookback():
    sql = "WHERE event_time >= now() - toIntervalHour({lookback_hours:UInt32})"
    result = inject_parameters(sql, cluster="c", lookback_hours=24, lookback_days=1)
    assert result == "WHERE event_time >= now() - toIntervalHour(24)"


def test_inject_parameters_lookback_days():
    sql = "WHERE event_time >= now() - toIntervalDay({lookback_days:UInt32})"
    result = inject_parameters(sql, cluster="c", lookback_hours=48, lookback_days=2)
    assert result == "WHERE event_time >= now() - toIntervalDay(2)"


def test_inject_parameters_skips_missing():
    sql = "SELECT * FROM system.processes"
    result = inject_parameters(sql, cluster="c", lookback_hours=6, lookback_days=1)
    assert result == "SELECT * FROM system.processes"
