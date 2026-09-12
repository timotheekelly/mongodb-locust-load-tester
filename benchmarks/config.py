"""Config loading: base.yaml + profile yaml + env var overrides.

Env var overrides use the pattern BENCH_<UPPER_SNAKE_KEY> for any top-level
scalar key (e.g. BENCH_MONGO_URI, BENCH_DOC_SIZE_BYTES, BENCH_TARGET_VOLUME_BYTES).
Values are parsed as YAML scalars so ints/floats/bools come through typed.
"""

import os
from pathlib import Path
from typing import Any

import yaml

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


def _deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _apply_env_overrides(cfg: dict) -> dict:
    for key in list(cfg.keys()):
        env_key = f"BENCH_{key.upper()}"
        if env_key in os.environ:
            raw = os.environ[env_key]
            try:
                cfg[key] = yaml.safe_load(raw)
            except yaml.YAMLError:
                cfg[key] = raw
    return cfg


def load_config(profile: str | None = None, overrides: dict[str, Any] | None = None) -> dict:
    """Load base config, merge a profile on top (if given), then env vars, then explicit overrides.

    `profile` is a name like "read_heavy", "balanced", "cpu_intensive" -- resolved to
    config/profile_<name>.yaml -- or a direct path to a yaml file.
    """
    base_path = CONFIG_DIR / "base.yaml"
    with open(base_path) as f:
        cfg = yaml.safe_load(f)

    if profile:
        profile_path = Path(profile)
        if not profile_path.exists():
            profile_path = CONFIG_DIR / f"profile_{profile}.yaml"
        if not profile_path.exists():
            raise FileNotFoundError(f"No profile config found for '{profile}' (looked in {profile_path})")
        with open(profile_path) as f:
            profile_cfg = yaml.safe_load(f) or {}
        cfg = _deep_merge(cfg, profile_cfg)

    cfg = _apply_env_overrides(cfg)

    if overrides:
        cfg = _deep_merge(cfg, overrides)

    return cfg


def resolve_volume(volume: str | int) -> int:
    """Accept a preset name (from base.yaml's volume_presets, e.g. tiny/small/medium/large)
    or a raw byte count."""
    if isinstance(volume, str) and not volume.isdigit():
        base_path = CONFIG_DIR / "base.yaml"
        with open(base_path) as f:
            presets = (yaml.safe_load(f) or {}).get("volume_presets", {})
        if volume not in presets:
            raise ValueError(
                f"Unknown volume preset '{volume}'; known presets: {sorted(presets.keys())}, "
                "or pass a raw byte count."
            )
        return int(presets[volume])
    return int(volume)
