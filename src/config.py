from pathlib import Path
import yaml


class ConfigError(Exception):
    pass


_REQUIRED_SECTIONS = ["clickhouse", "lookback", "refresh", "alerts", "thresholds"]
_REQUIRED_CLICKHOUSE = ["host", "port", "user", "password", "cluster", "connect_timeout", "query_timeout"]
_REQUIRED_SMTP = ["host", "port", "use_tls", "user", "password", "from_address", "recipients"]


def load_config(path: Path) -> dict:
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    with open(path) as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict):
        raise ConfigError("Config file is empty or invalid YAML")

    for section in _REQUIRED_SECTIONS:
        if section not in cfg:
            raise ConfigError(f"Missing required section: {section}")

    for field in _REQUIRED_CLICKHOUSE:
        if field not in cfg["clickhouse"]:
            raise ConfigError(f"Missing required clickhouse field: {field}")

    if cfg["alerts"].get("enabled"):
        smtp = cfg["alerts"].get("smtp", {})
        for field in _REQUIRED_SMTP:
            if field not in smtp:
                raise ConfigError(f"Missing required smtp field: {field}")

    return cfg
