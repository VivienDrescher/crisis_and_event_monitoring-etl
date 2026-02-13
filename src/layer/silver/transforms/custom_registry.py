from src.layer.silver.transforms.custom.acled import build as acled
from src.layer.silver.transforms.custom.gdelt import build as gdelt

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
