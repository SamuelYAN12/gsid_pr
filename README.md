# GSID Integrator (Focused Set)

This is a minimal, runnable skeleton to integrate six instrument types:
**EQUITY, FUTURES, OPTIONS, INDEX, BOND, FUND_EXCH**.

## Quickstart
```bash
cd gsid_integrator
python -m pip install -r requirements.txt
python -m src.gsid_integrator   # or: python -m gsid_integrator (if installed as a package)
```
Outputs will appear in `data/curated/master/security_master/` and split by buckets under `data/curated/master/*`.
Sample input data is provided for date `2025-10-15` under `data/raw/*/2025-10-15/`.
