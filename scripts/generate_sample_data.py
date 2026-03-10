"""Generate simulated sample data for IMPACT Entity Data Module demos.

Creates realistic lending data across three entity levels:

- **Obligor** (borrower): 10 companies with financial attributes
- **Facility** (loan): ~25 facilities spread across obligors
- **Collateral**: ~40 collateral positions pledged against facilities
- **Rating overrides**: ~8 manual rating overrides

Data is saved in multiple formats to demonstrate different source types:

- ``data/sample/lending.db`` — SQLite database (obligor_main, facility_main tables)
- ``data/sample/collateral.parquet`` — Parquet file
- ``data/sample/rating_overrides.csv`` — CSV file

The demo configs in ``configs/demo/`` reference these files directly,
so no Snowflake connection is needed to run the full pipeline.

Usage::

    python scripts/generate_sample_data.py

After running, use the demo configs::

    from impact.entity.pipeline import EntityPipeline

    # Facility-level pipeline
    result = EntityPipeline("configs/demo/facility_demo.yaml").run()

    # Obligor-level pipeline (with nested facilities and collateral)
    result = EntityPipeline("configs/demo/obligor_demo.yaml").run()
"""

from __future__ import annotations

import random
import sqlite3
from pathlib import Path

import pandas as pd

# Reproducible
random.seed(42)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def generate_obligors(n: int = 10) -> pd.DataFrame:
    """Generate obligor (borrower) records."""
    industries = ["Technology", "Healthcare", "Manufacturing", "Energy", "Retail",
                  "Financial Services", "Real Estate", "Telecom"]
    countries = ["US", "US", "US", "UK", "CA", "DE"]  # weighted toward US

    rows = []
    for i in range(1, n + 1):
        revenue = round(random.uniform(5_000_000, 500_000_000), 2)
        total_assets = round(revenue * random.uniform(1.5, 4.0), 2)
        # Most obligors have liabilities < assets, a few are over-leveraged
        liability_ratio = random.uniform(0.3, 0.9) if i % 4 != 0 else random.uniform(1.05, 1.3)
        total_liabilities = round(total_assets * liability_ratio, 2)

        rows.append({
            "obligor_id": f"OBL-{i:03d}",
            "legal_name": f"Acme {industries[i % len(industries)]} Corp #{i}",
            "industry": industries[i % len(industries)],
            "country": random.choice(countries),
            "revenue": revenue,
            "total_assets": total_assets,
            "total_liabilities": total_liabilities,
            "founded_date": f"{random.randint(1985, 2015)}-{random.randint(1, 12):02d}-01",
            "snapshot_date": "2025-12-31",
        })

    return pd.DataFrame(rows)


def generate_facilities(obligors: pd.DataFrame, avg_per_obligor: float = 2.5) -> pd.DataFrame:
    """Generate facility (loan) records linked to obligors."""
    products = ["TERM_LOAN", "TERM_LOAN", "REVOLVER", "REVOLVER", "LOC"]  # weighted
    rows = []
    fac_id = 1

    for _, obligor in obligors.iterrows():
        n_facilities = max(1, int(random.gauss(avg_per_obligor, 1)))
        for _ in range(n_facilities):
            commitment = round(random.uniform(100_000, 50_000_000), 2)
            utilization = random.uniform(0.1, 0.95)
            outstanding = round(commitment * utilization, 2)
            rate = round(random.uniform(0.02, 0.12), 4)

            orig_year = random.randint(2018, 2024)
            orig_month = random.randint(1, 12)
            term_years = random.choice([3, 5, 7, 10])
            mat_year = orig_year + term_years

            rows.append({
                "facility_id": f"FAC-{fac_id:04d}",
                "obligor_id": obligor["obligor_id"],
                "product_type": random.choice(products),
                "commitment_amount": commitment,
                "outstanding_balance": outstanding,
                "origination_date": f"{orig_year}-{orig_month:02d}-15",
                "maturity_date": f"{mat_year}-{orig_month:02d}-15",
                "interest_rate": rate,
                "snapshot_date": "2025-12-31",
            })
            fac_id += 1

    return pd.DataFrame(rows)


def generate_collateral(facilities: pd.DataFrame, coverage_pct: float = 0.7) -> pd.DataFrame:
    """Generate collateral positions linked to facilities.

    Args:
        facilities: Facility DataFrame.
        coverage_pct: Fraction of facilities that have collateral.
    """
    collateral_types = ["REAL_ESTATE", "EQUIPMENT", "INVENTORY", "RECEIVABLES", "CASH"]
    rows = []

    for _, fac in facilities.iterrows():
        if random.random() > coverage_pct:
            continue  # some facilities have no collateral

        n_items = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
        for _ in range(n_items):
            value = round(fac["commitment_amount"] * random.uniform(0.2, 0.8), 2)
            rows.append({
                "facility_id": fac["facility_id"],
                "collateral_type": random.choice(collateral_types),
                "collateral_value": value,
            })

    return pd.DataFrame(rows)


def generate_rating_overrides(facilities: pd.DataFrame, override_pct: float = 0.3) -> pd.DataFrame:
    """Generate rating override records for a subset of facilities."""
    ratings = ["AAA", "AA", "A", "BBB", "BB", "B"]
    rows = []

    for _, fac in facilities.iterrows():
        if random.random() > override_pct:
            continue
        rows.append({
            "facility_id": fac["facility_id"],
            "product_type": fac["product_type"],
            "rating_override": random.choice(ratings),
        })

    return pd.DataFrame(rows)


def save_sqlite(obligors: pd.DataFrame, facilities: pd.DataFrame, db_path: Path):
    """Save obligor and facility data to a SQLite database."""
    conn = sqlite3.connect(str(db_path))
    obligors.to_sql("obligor_main", conn, if_exists="replace", index=False)
    facilities.to_sql("facility_main", conn, if_exists="replace", index=False)
    conn.close()


def main():
    print("Generating sample data...")

    obligors = generate_obligors(10)
    facilities = generate_facilities(obligors)
    collateral = generate_collateral(facilities)
    rating_overrides = generate_rating_overrides(facilities)

    # SQLite — obligor and facility tables (simulates Snowflake)
    db_path = OUTPUT_DIR / "lending.db"
    save_sqlite(obligors, facilities, db_path)

    # Parquet — collateral
    parquet_path = OUTPUT_DIR / "collateral.parquet"
    collateral.to_parquet(parquet_path, index=False)

    # CSV — rating overrides
    csv_path = OUTPUT_DIR / "rating_overrides.csv"
    rating_overrides.to_csv(csv_path, index=False)

    # Also save all as CSV for easy inspection
    obligors.to_csv(OUTPUT_DIR / "obligor_main.csv", index=False)
    facilities.to_csv(OUTPUT_DIR / "facility_main.csv", index=False)
    collateral.to_csv(OUTPUT_DIR / "collateral.csv", index=False)

    print(f"  Obligors:         {len(obligors):>3} records")
    print(f"  Facilities:       {len(facilities):>3} records")
    print(f"  Collateral:       {len(collateral):>3} records")
    print(f"  Rating overrides: {len(rating_overrides):>3} records")
    print()
    print(f"  SQLite database:  {db_path}")
    print(f"  Parquet file:     {parquet_path}")
    print(f"  CSV files:        {OUTPUT_DIR}/*.csv")
    print()
    print("Done. Run the demo with:")
    print('  EntityPipeline("configs/demo/facility_demo.yaml").run()')


if __name__ == "__main__":
    main()
