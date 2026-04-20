import pandas as pd
from src.ui.formatters import severity_color, colorize_row


def test_severity_color_critical():
    assert severity_color("CRITICAL") == "background-color: #ffcccc"


def test_severity_color_warning():
    assert severity_color("WARNING") == "background-color: #ffe0b2"


def test_severity_color_caution():
    assert severity_color("CAUTION") == "background-color: #fff9c4"


def test_severity_color_ok():
    assert severity_color("OK") == ""


def test_severity_color_unknown():
    assert severity_color("UNKNOWN") == ""


def test_colorize_row_finds_severity():
    row = pd.Series({
        "hostname": "node1",
        "replica_status": "CRITICAL - Replica is read-only",
    })
    result = colorize_row(row, status_column="replica_status")
    assert all(v == "background-color: #ffcccc" for v in result)


def test_colorize_row_ok_no_color():
    row = pd.Series({
        "hostname": "node1",
        "replica_status": "OK       - Replica is healthy",
    })
    result = colorize_row(row, status_column="replica_status")
    assert all(v == "" for v in result)
