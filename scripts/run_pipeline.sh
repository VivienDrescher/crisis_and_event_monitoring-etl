#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( dirname "$SCRIPT_DIR" )"

if [ -f "$PROJECT_DIR/pipeline.lock" ]; then
    echo "Pipeline already running"
    exit 1
fi

source "$PROJECT_DIR/.venv/bin/activate"
cd "$PROJECT_DIR"

python -m src.orchestrator
