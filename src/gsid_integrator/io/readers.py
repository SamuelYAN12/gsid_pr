import glob, pandas as pd

def read_source_batch(source_name: str, file_patterns):
    files = []
    for pat in file_patterns if isinstance(file_patterns, list) else [file_patterns]:
        files.extend(glob.glob(pat))
    if not files:
        return pd.DataFrame()

    chunks = []
    for f in files:
        if f.lower().endswith(".csv"):
            chunks.append(pd.read_csv(f))
        else:
            chunks.append(pd.read_parquet(f))
    df = pd.concat(chunks, ignore_index=True)
    df["source"] = source_name
    return df
