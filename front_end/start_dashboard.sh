#!/bin/bash

# Start BTC Dashboard (Reflex App)

echo "Starting BTC Dashboard..."

# Activate virtual environment if exists
if [ -d "../.venv" ]; then
    source ../.venv/bin/activate
fi

# Install dependencies
pip install -r requirements.txt

# Run the Reflex app
reflex run --loglevel info
