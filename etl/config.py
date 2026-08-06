import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Settings:
    database_url: str
    data_dir: Path


def get_settings() -> Settings:
    database_url = os.environ.get(
        "DATABASE_URL", "postgresql://ecommerce:ecommerce@localhost:5434/ecommerce"
    )
    data_dir = Path(os.environ.get("DATA_DIR", REPO_ROOT / "ecommerce_data"))
    return Settings(database_url=database_url, data_dir=data_dir)
