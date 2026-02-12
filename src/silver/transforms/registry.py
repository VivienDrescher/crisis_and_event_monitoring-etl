from src.silver.transforms.acled import build as acled
from src.silver.transforms.gdelt import build as gdelt

SILVER_DATASET_CUSTOM_TRANSFORMS = {
    "acled": {
        "function": acled,
        "name": "acled",
    },
    "gdelt": {
        "function": gdelt,
        "name": "gdelt",
    },
}
