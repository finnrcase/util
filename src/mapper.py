"""
ZIP-to-region mapping logic for Util.
"""

from pathlib import Path

import pandas as pd


def load_zip_region_map(filepath: str | Path) -> pd.DataFrame:
    """
    Load ZIP-to-region mapping table from CSV.

    Expected columns:
    - zip_code
    - region
    """
    df = pd.read_csv(filepath, dtype={"zip_code": str})

    required_columns = {"zip_code", "region"}
    if not required_columns.issubset(df.columns):
        raise ValueError(
            f"ZIP-region mapping file must contain columns: {required_columns}"
        )

    return df


def map_zip_to_region(zip_code: str, mapping_df: pd.DataFrame) -> str:
    """
    Map a ZIP code to a region.
    """
    zip_code = str(zip_code).strip()

    match = mapping_df.loc[mapping_df["zip_code"] == zip_code, "region"]

    if match.empty:
        raise ValueError(f"No region mapping found for ZIP code: {zip_code}")

    return match.iloc[0]