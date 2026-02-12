from __future__ import annotations

from pathlib import Path
from typing import Optional
import logging
import shutil 

    
def clear_data_dir(
        data_dir: Path, 
        logger: Optional[logging.Logger] = None,
) -> None:
    
    logger = logger or logging.getLogger(__name__)

    for path in data_dir.glob("*"):
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    logger.info("Cleared existing data directory")