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

    # Manually-known peers loaded into the crawl queue on startup.
    # Each entry: {ip: str, port: int, source: str (optional, operator-only metadata)}
    static_peers: list[dict] = field(default_factory=list)

    api_port: int = 8025
    api_host: str = "0.0.0.0"
    api_max_results: int = 25
    api_max_age_hours: int = 6

    # Ranking
    ranking_window_days: int = 7
    ranking_prior_attempts: int = 10
    ranking_prior_successes: int = 5
    ranking_longevity_cap_days: int = 60
    ranking_longevity_weight: float = 0.30
    ranking_inclusion_threshold: float = 0.50

    # ---- HOLD-AND-SEE PROBE (default OFF) --------------------------------
    # uptime_score only records whether a crawl probe SUCCEEDED. A node at
    # maxconnections still accepts a probe -- it evicts some other peer to do it --
    # so no existing metric can see saturation. This probe measures the thing a
    # wallet actually experiences: connect, hold, and see whether we get dropped.
    #
    # OFF by default and deliberately so: holding a socket occupies a slot on the
    # very nodes that are short of them, and on a saturated node our connect costs
    # somebody else theirs. Keep hold_probe_seconds small and concurrency low.
    hold_probe_enabled: bool = False
    hold_probe_seconds: int = 60
    hold_probe_concurrency: int = 4
    hold_probe_timeout: int = 5

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
