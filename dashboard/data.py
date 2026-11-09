import os

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

BUCKET = os.getenv("S3_BUCKET", "lakehouse")

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT", "http://localhost:8333"),
    "AWS_ACCESS_KEY_ID": os.getenv("S3_ACCESS_KEY", "lakehouse"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("S3_SECRET_KEY", "lakehouse-secret"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}


def _table(name: str) -> DeltaTable:
    return DeltaTable(f"s3://{BUCKET}/gold/{name}", storage_options=STORAGE_OPTIONS)


@st.cache_data(ttl=600)
def load(name: str, filters: tuple | None = None, columns: tuple | None = None) -> pd.DataFrame:
    dt = _table(name)
    return dt.to_pandas(
        filters=list(filters) if filters else None,
        columns=list(columns) if columns else None,
    )


@st.cache_data(ttl=600)
def available_years() -> list[int]:
    # only years with at least one full (representative) station-year, the current year
    # from the live API is too short for annual stats
    df = load("annual_compliance", columns=("year", "is_representative"))
    return sorted(df.loc[df["is_representative"], "year"].unique().tolist())
