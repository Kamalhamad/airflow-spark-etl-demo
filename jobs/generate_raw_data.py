from __future__ import annotations

import argparse
import os
import pandas as pd
from datetime import date, timedelta
import random

def main(out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    countries = ["DE", "FR", "NL", "SE", "ES"]
    statuses = ["active", "inactive"]
    base = date(2023, 1, 1)

    rows = []
    for i in range(1, 501):
        rows.append({
            "customer_id": i,
            "country": random.choice(countries),
            "signup_date": str(base + timedelta(days=random.randint(0, 600))),
            "status": random.choice(statuses),
        })

    pd.DataFrame(rows).to_csv(out_path, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", dest="out_path", required=True)
    args = parser.parse_args()
    main(args.out_path)
