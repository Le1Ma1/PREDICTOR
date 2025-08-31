import os
from pathlib import Path

PROJ = Path(__file__).resolve().parents[2]

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def data_raw(asset: str, freq: str) -> Path:
    return ensure_dir(PROJ / "data" / "raw" / asset / freq)

def data_proc(asset: str, freq: str) -> Path:
    return ensure_dir(PROJ / "data" / "processed" / asset / freq)

def artifacts(model: str, exp: str) -> Path:
    return ensure_dir(PROJ / f"artifacts_{model}" / exp)
