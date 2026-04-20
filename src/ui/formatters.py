import pandas as pd
from src.alerts.evaluator import extract_severity


_SEVERITY_COLORS = {
    "CRITICAL": "background-color: #ffcccc",
    "WARNING": "background-color: #ffe0b2",
    "CAUTION": "background-color: #fff9c4",
}


def severity_color(severity: str) -> str:
    return _SEVERITY_COLORS.get(severity, "")


def colorize_row(row: pd.Series, status_column: str) -> list:
    value = row.get(status_column, "")
    severity = extract_severity(value)
    color = severity_color(severity) if severity else ""
    return [color] * len(row)


def style_dataframe(df: pd.DataFrame, status_column: str):
    if status_column not in df.columns:
        return df.style
    return df.style.apply(colorize_row, axis=1, status_column=status_column)
