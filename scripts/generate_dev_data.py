"""Generates a small-scale dataset for local development/demo purposes.

data-generator.py defaults to the full 1M-customer/5M-order/20M-item dataset,
which is too slow to regenerate on every dev iteration. This script runs it
with much smaller sizes (via the env var overrides added to data-generator.py)
so `ecommerce_data/*.csv` and `ecommerce_data/sample_*.csv` are ready in seconds.
"""
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

DEV_SIZES = {
    "NUM_CATEGORIES": "500",
    "NUM_PRODUCTS": "2000",
    "NUM_CUSTOMERS": "2000",
    "NUM_ORDERS": "5000",
    "NUM_ORDER_ITEMS": "15000",
}


def main() -> None:
    env = os.environ.copy()
    env.update(DEV_SIZES)
    subprocess.run(
        [sys.executable, str(REPO_ROOT / "data-generator.py")],
        cwd=REPO_ROOT,
        env=env,
        check=True,
    )


if __name__ == "__main__":
    main()
