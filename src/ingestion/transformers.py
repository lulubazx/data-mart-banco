import pandas as pd


def convert_date_columns(df, table_cfg, logger=None):
    date_columns = table_cfg.get("date_columns", [])
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            # Normalize precision to ns for consistent downstream behavior/tests.
            if hasattr(df[col].dt, "as_unit"):
                df[col] = df[col].dt.as_unit("ns")
            else:
                df[col] = df[col].astype("datetime64[ns, UTC]")
            if df[col].isna().any() and logger:
                logger.warning("Date coercion produced nulls: column=%s", col)
    return df
