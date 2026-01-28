from __future__ import annotations

from pathlib import Path
import logging
import requests
import zipfile
from typing import Optional


def download_and_extract(
    url: str,
    target_path: Path,
    expected_suffix: str,
    timeout: int = 60,
    logger: Optional[logging.Logger] = None
) -> Path:
    """
    Download a file from a URL and optionally extract if it is a ZIP archive.

    Notes:
        - If the file is a ZIP, only returns the file inside with the expected suffix.
        - The original ZIP file is removed after extraction.
        - Raises RuntimeError if the ZIP does not contain exactly one expected file.

    Args:
        url: Remote file URL
        target_path: Local path where file will be saved
        expected_suffix: Expected extension of final file (e.g. '.csv', '.xlsx')
        timeout: Timeout for HTTP request (seconds)
        logger: Optional logger. Defaults to module logger.

    Returns:
        Path to the downloaded (and possibly extracted) file. May differ from `target_path` if ZIP extracted.
    """
    logger = logger or logging.getLogger(__name__)
    expected_suffix = expected_suffix.lower()

    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    target_path.write_bytes(response.content)
    logger.info(f"[download_and_extract_file] Download complete: {target_path}")

    # Handle ZIP extraction
    if target_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(target_path, "r") as z:
            z.extractall(target_path.parent)
        target_path.unlink()
        logger.info(f"[download_and_extract_file] ZIP extracted into {target_path.parent}")

        # Identify expected file
        extracted_files = [
            p for p in target_path.parent.iterdir()
            if p.is_file() and p.suffix.lower() == expected_suffix
        ]
        if len(extracted_files) != 1:
            raise RuntimeError(
                f"[download_and_extract] Expected 1 file with suffix '{expected_suffix}' in ZIP, "
                f"found {len(extracted_files)}: {[p.name for p in extracted_files]}"
            )

        target_path = extracted_files[0]

    return target_path