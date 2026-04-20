"""
One-time script to generate the full US ZIP code coordinate dataset.

Run this once after setting up the project:
    python scripts/generate_zip_coords.py

This creates data/raw/us_zip_coords.csv from the pgeocode/GeoNames dataset.
That file is used by zip_resolver.py as a fast local lookup so the app never
needs to download data at runtime.

Requirements: pgeocode must be installed (pip install pgeocode).
The first run will download the GeoNames US dataset (~5 MB) to ~/.pgeocode/.
Subsequent runs use the cached download.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / "us_zip_coords.csv"


def main() -> None:
    try:
        import pgeocode
    except ImportError:
        print("ERROR: pgeocode is not installed. Run: pip install pgeocode", file=sys.stderr)
        sys.exit(1)

    print("Loading pgeocode US dataset (may download on first run)...")
    nomi = pgeocode.Nominatim("US")

    df = nomi._data[["postal_code", "country_code", "latitude", "longitude"]].copy()
    df = df.dropna(subset=["latitude", "longitude"])
    df["postal_code"] = df["postal_code"].astype(str).str.strip().str.zfill(5)
    df["country_code"] = "US"
    df = df.sort_values("postal_code").reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {len(df):,} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
