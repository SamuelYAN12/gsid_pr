import pandas as pd

def _assign_segment_base(itype: str) -> int:
    base = {
        "EQUITY": 1_000_000,
        "INDEX":  2_000_000,
        "BOND":   3_000_000,
        "FUND_EXCH": 4_000_000,
        "FUTURES": 5_000_000,
        "OPTIONS": 6_000_000
    }
    return base.get(itype, 9_000_000)

def merge_master(std_df: pd.DataFrame, merge_cfg: dict) -> pd.DataFrame:
    if std_df is None or std_df.empty:
        return pd.DataFrame()

    dfs = []
    key_map = {
        "EQUITY": ["symbol","exchange"],
        "INDEX": ["symbol","exchange"],
        "BOND": ["symbol","exchange"],
        "FUND_EXCH": ["symbol","exchange"],
        "FUTURES": ["symbol","exchange","contract_month"],
        "OPTIONS": ["symbol","exchange","expiry","strike","call_put"],
    }

    for itype, keys in key_map.items():
        d = std_df[std_df["instrument_type"] == itype].copy()
        if d.empty:
            continue
        for col in keys:
            if col not in d.columns:
                d[col] = pd.NA

        # source priority and latest updated_at wins
        prio = {s:i for i, s in enumerate(merge_cfg.get("source_priority", []))}
        d["__prio__"] = d["source"].map(prio).fillna(99).astype(int)
        sort_cols = ["__prio__"]
        tie = merge_cfg.get("tie_breaker", "updated_at")
        if tie in d.columns:
            sort_cols.append(tie)
        d = d.sort_values(by=sort_cols, ascending=[True, False])

        d = d.drop_duplicates(subset=keys, keep="first")

        d = d.reset_index(drop=True)
        # deterministic GSID per partition + offset segment
        d["gsid"] = d.groupby(keys, sort=False).ngroup() + 1 + _assign_segment_base(itype)

        dfs.append(d)

    if not dfs:
        return pd.DataFrame()

    out = pd.concat(dfs, ignore_index=True)

    # ensure optional columns exist
    for c in ["contract_month","expiry","strike","call_put","underlying_symbol"]:
        if c not in out.columns: out[c] = pd.NA

    cols = ["gsid","instrument_type","symbol","exchange","name","isin","currency",
            "listed_date","delisted_date","contract_month","expiry","strike","call_put",
            "underlying_symbol","updated_at"]
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out[cols]
