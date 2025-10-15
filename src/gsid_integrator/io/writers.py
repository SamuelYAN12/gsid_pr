from pathlib import Path

def ensure_parent(p: str):
    Path(p).parent.mkdir(parents=True, exist_ok=True)
