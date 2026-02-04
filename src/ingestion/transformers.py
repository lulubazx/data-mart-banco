import pandas as pd


def convert_date_columns(df, table_cfg, logger=None):
    date_columns = table_cfg.get("date_columns", [])
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            if df[col].isna().any() and logger:
                logger.warning("Date coercion produced nulls: column=%s", col)
    return df
