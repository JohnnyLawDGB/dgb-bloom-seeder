import yaml
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Config:
    dgb_port: int = 12024
    dgb_magic: bytes = b"\xfa\xc3\xb6\xda"

    crawl_interval: int = 1800
    crawl_concurrency: int = 10
    crawl_timeout: int = 5
    crawl_max_peers: int = 500
    prune_hours: int = 24

    dns_seeds: list[str] = field(default_factory=lambda: [
        "seed.digibyte.io",
        "seed2.digibyte.io",
        "seed.digibyteprojects.com",
        "digibyteblockexplorer.com",
        "dgbseed.org",
    ])

    api_port: int = 8025
    api_host: str = "0.0.0.0"
    api_max_results: int = 25
    api_max_age_hours: int = 6

    db_path: str = "bloom_seeder.db"
    log_level: str = "INFO"


def load_config(path: str = "config.yaml") -> Config:
    p = Path(path)
    if not p.exists():
        return Config()
    with open(p) as f:
        data = yaml.safe_load(f) or {}
    cfg = Config()
    for key, val in data.items():
        if key == "dgb_magic":
            cfg.dgb_magic = bytes.fromhex(val)
        elif hasattr(cfg, key):
            setattr(cfg, key, val)
    return cfg
