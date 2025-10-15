import yaml, pandas as pd

def load_yaml(p):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _apply_mapping(df: pd.DataFrame, mapping: dict, source: str):
    src_map = mapping["sources"][source]
    out = pd.DataFrame()
    for canon, spec in mapping["canonical"].items():
        src_col = src_map.get(canon)
        if src_col is None:
            out[canon] = pd.NA
        else:
            out[canon] = df[src_col] if src_col in df.columns else pd.NA
    return out

def _split_windcode(df: pd.DataFrame):
    parts = df["symbol"].astype(str).str.split(".", n=1, expand=True)
    df["symbol"] = parts[0]
    df["exchange"] = parts[1]
    return df

def normalize_sources(dfs, mapping_yaml, dicts_yaml="src/gsid_integrator/rules/dicts/instrument_map.yaml", filters=None):
    mapping = load_yaml(mapping_yaml)
    dicts = load_yaml(dicts_yaml)
    allowed = set((filters or {}).get("allowed_instruments", []))
    allowed_ex = set((filters or {}).get("allowed_exchanges", []))

    frames = []
    for source, df in dfs:
        if df is None or df.empty:
            continue
        # map columns
        out = _apply_mapping(df, mapping, source)

        # wind code split
        if source == "wind":
            out = _split_windcode(out)

        # instrument type mapping
        itmap = dicts["instrument_type_map"].get(source, {})
        out["instrument_type"] = out["instrument_type"].map(itmap).fillna(out["instrument_type"])

        # exchange map (only map if present)
        exmap = dicts.get("exchange_map", {})
        out["exchange"] = out["exchange"].map(exmap).fillna(out["exchange"])

        # coerce dtypes
        for dt_col in ["listed_date","delisted_date","updated_at"]:
            if dt_col in out.columns:
                out[dt_col] = pd.to_datetime(out[dt_col], errors="coerce")

        # attach source name for priority
        out["source"] = source

        # filtering
        if allowed:
            out = out[out["instrument_type"].isin(allowed)]
        if allowed_ex:
            out = out[out["exchange"].isin(allowed_ex) | out["exchange"].isna()]

        frames.append(out)

    if not frames:
        return pd.DataFrame(columns=list(mapping["canonical"].keys()) + ["source"])

    std = pd.concat(frames, ignore_index=True)
    return std
