#!/bin/bash

echo "Starting Streamlit Dashboard..."
cd dashboard
streamlit run streamlit_dashboard.py --server.port=${PORT:-8501} --server.address=0.0.0.0 --server.headless=true
