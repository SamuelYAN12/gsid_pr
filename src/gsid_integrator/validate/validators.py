import pandas as pd

def validate_master(df: pd.DataFrame):
    if df is None or df.empty:
        raise ValueError("No rows produced in master output.")

    # basic uniqueness on gsid
    if df["gsid"].isna().any():
        raise ValueError("GSID contains NA values.")
    if df["gsid"].duplicated().any():
        raise ValueError("GSID has duplicates.")

    # instrument-specific sanity
    fut = df[df["instrument_type"] == "FUTURES"]
    if len(fut) and fut["contract_month"].isna().any():
        raise ValueError("FUTURES rows must have contract_month.")

    opt = df[df["instrument_type"] == "OPTIONS"]
    if len(opt):
        missing = opt["expiry"].isna() | opt["strike"].isna() | opt["call_put"].isna()
        if missing.any():
            raise ValueError("OPTIONS rows must have expiry, strike, call_put.")

    return True
