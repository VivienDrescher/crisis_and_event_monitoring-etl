from src.layer.gold.transforms.custom.acled_monthly import build as acled_monthly
from src.layer.gold.transforms.custom.conflicts_monthly import (
    build as conflicts_monthly,
)
from src.layer.gold.transforms.custom.gdelt_daily import build as gdelt_daily
from src.layer.gold.transforms.custom.gdelt_monthly import build as gdelt_monthly

GOLD_DATASET_TRANSFORMS = {
    "gdelt_daily": {
        "function": gdelt_daily,
        "name": "gdelt_daily",
    },
    "gdelt_monthly": {
        "function": gdelt_monthly,
        "name": "gdelt_monthly",
    },
    "acled_monthly": {
        "function": acled_monthly,
        "name": "acled_monthly",
    },
    "conflicts_monthly": {
        "function": conflicts_monthly,
        "name": "conflicts_monthly",
    },
}
