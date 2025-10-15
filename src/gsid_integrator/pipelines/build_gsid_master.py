import os, glob, yaml, pandas as pd
from pathlib import Path
from ..io.readers import read_source_batch
from ..transforms.normalize import normalize_sources
from ..transforms.merge import merge_master
from ..validate.validators import validate_master

def load_yaml(p):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run_pipeline(config_path="src/gsid_integrator/config/app.yaml"):
    cfg = load_yaml(config_path)
    date = cfg["input"]["date"]

    # 1) Read three sources from raw CSVs
    dfs = []
    for s in cfg["sources"]:
        pattern = s["path"].format(base_dir=cfg["input"]["base_dir"], date=date)
        files = glob.glob(pattern)
        df = read_source_batch(s["name"], files)
        if not df.empty:
            dfs.append((s["name"], df))

    # 2) Normalize + filter to the six instrument types
    std_df = normalize_sources(dfs, "src/gsid_integrator/rules/mapping_fields.yaml",
                               filters=cfg.get("filters", {}))

    # 3) Merge with source priority + latest updated_at
    master = merge_master(std_df, cfg["merge"])

    # 4) Validate
    validate_master(master)

    # 5) Write outputs (full & partition, plus buckets)
    out_full = cfg["output"]["gsid_csv"]
    out_part = cfg["output"]["partition_dir"].format(date=date)
    Path(os.path.dirname(out_full)).mkdir(parents=True, exist_ok=True)
    Path(os.path.dirname(out_part)).mkdir(parents=True, exist_ok=True)
    master.to_csv(out_full, index=False)
    master.to_csv(out_part, index=False)

    buckets = cfg["output"].get("buckets", {})
    for itype, path in buckets.items():
        sub = master[master["instrument_type"] == itype]
        if len(sub):
            Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
            sub.to_csv(path, index=False)

    print(f"GSID master rows: {len(master)}")

if __name__ == "__main__":
    run_pipeline()
