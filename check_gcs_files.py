#!/usr/bin/env python3
"""Check what files exist in GCS bucket"""

from google.cloud import storage
import os
import json

# Load credentials
creds_json = open('/home/daniellai/confluent-gcs-key.json').read()
from google.oauth2 import service_account
creds_dict = json.loads(creds_json)
credentials = service_account.Credentials.from_service_account_info(creds_dict)

# Connect to GCS
client = storage.Client(credentials=credentials, project='crypto-dp')
bucket = client.bucket('crypto-db-east1')

# List recent files
print("📂 Listing ALL files in gs://crypto-db-east1/...")
blobs = list(bucket.list_blobs(prefix='year=2025/month=11/day=27/', max_results=100))

print(f"\n✅ Found {len(blobs)} files\n")

for blob in sorted(blobs, key=lambda x: x.time_created, reverse=True)[:10]:
    print(f"  📄 {blob.name}")
    print(f"     Created: {blob.time_created}")
    print()
