import requests
import zipfile
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def download_and_extract(url: str, target_path: Path, expected_suffix: str, timeout: int = 60) -> Path:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    with open(target_path, "wb") as f:
        f.write(response.content)
    logger.info(f"Downloaded {target_path}")

    if target_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(target_path, "r") as z:
            z.extractall(target_path.parent)
        logger.info(f"Extracted ZIP contents to {target_path.parent}")
        target_path.unlink()

        # Pick the extracted file with expected extension
        extracted_files = [p for p in target_path.parent.iterdir() if p.is_file() and p.suffix.lower() == expected_suffix]
        if len(extracted_files) != 1:
            raise RuntimeError(f"Expected 1 {expected_suffix} in ZIP, found {len(extracted_files)}: {[p.name for p in extracted_files]}")
        return extracted_files[0]

    return target_path
