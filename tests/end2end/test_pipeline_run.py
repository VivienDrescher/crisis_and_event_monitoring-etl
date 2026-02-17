import subprocess
import sys


def test_full_pipeline(tmp_path):
    # Run orchestrator in-process or via subprocess
    result = subprocess.run([sys.executable, "-m", "src.orchestrator"], check=True)
    assert result.returncode == 0
    # Optionally check outputs exist in tmp_path / gold / silver directories
