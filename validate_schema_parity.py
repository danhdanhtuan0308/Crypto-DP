"""
Validate schema parity between Kafka pipeline and Batch pipeline outputs.

Kafka Pipeline: gs://crypto-db-east1/RealTime/year=2025/**/*.parquet
Batch Pipeline: gs://batch-btc-1h-east1/**/*.parquet

This script compares:
1. Column names and data types
2. Sample data ranges
3. Feature completeness
"""

import pyarrow.parquet as pq
import pyarrow as pa
from google.cloud import storage
import io
import sys

def get_multiple_kafka_files(bucket_name, prefix, num_files=5):
    """Get multiple recent Kafka files to ensure we have enough rows."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    # Filter for parquet files
    parquet_blobs = [b for b in blobs if b.name.endswith('.parquet')]
    
    if not parquet_blobs:
        raise ValueError(f"No parquet files found in gs://{bucket_name}/{prefix}")
    
    # Sort by time_created descending
    parquet_blobs.sort(key=lambda x: x.time_created, reverse=True)
    
    return parquet_blobs[:num_files]

def get_latest_file(bucket_name, prefix):
    """Get the most recent parquet file from a GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    # Filter for parquet files
    parquet_blobs = [b for b in blobs if b.name.endswith('.parquet')]
    
    if not parquet_blobs:
        raise ValueError(f"No parquet files found in gs://{bucket_name}/{prefix}")
    
    # Sort by time_created descending
    parquet_blobs.sort(key=lambda x: x.time_created, reverse=True)
    
    return parquet_blobs[0]

def download_and_read_parquet(bucket_name, blob_name):
    """Download parquet file from GCS and read as PyArrow table."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    data = blob.download_as_bytes()
    table = pq.read_table(io.BytesIO(data))
    
    return table

def compare_schemas(kafka_table, batch_table):
    """Compare schemas between Kafka and Batch pipeline outputs."""
    print("\n" + "=" * 80)
    print("SCHEMA COMPARISON")
    print("=" * 80)
    
    kafka_schema = kafka_table.schema
    batch_schema = batch_table.schema
    
    kafka_cols = set(kafka_schema.names)
    batch_cols = set(batch_schema.names)
    
    print(f"\n📊 Kafka Pipeline Columns: {len(kafka_cols)}")
    print(f"📊 Batch Pipeline Columns: {len(batch_cols)}")
    
    # Check for missing columns
    missing_in_batch = kafka_cols - batch_cols
    missing_in_kafka = batch_cols - kafka_cols
    common_cols = kafka_cols & batch_cols
    
    if missing_in_batch:
        print(f"\n❌ Columns in Kafka but MISSING in Batch ({len(missing_in_batch)}):")
        for col in sorted(missing_in_batch):
            print(f"   - {col}")
    
    if missing_in_kafka:
        print(f"\n⚠️  Columns in Batch but NOT in Kafka ({len(missing_in_kafka)}):")
        for col in sorted(missing_in_kafka):
            print(f"   - {col}")
    
    print(f"\n✅ Common Columns: {len(common_cols)}")
    
    # Compare data types for common columns
    print("\n" + "=" * 80)
    print("DATA TYPE COMPARISON (Common Columns)")
    print("=" * 80)
    
    type_mismatches = []
    for col in sorted(common_cols):
        kafka_type = str(kafka_schema.field(col).type)
        batch_type = str(batch_schema.field(col).type)
        
        if kafka_type != batch_type:
            type_mismatches.append((col, kafka_type, batch_type))
    
    if type_mismatches:
        print(f"\n⚠️  Type Mismatches ({len(type_mismatches)}):")
        for col, k_type, b_type in type_mismatches:
            print(f"   {col}: Kafka={k_type} vs Batch={b_type}")
    else:
        print("\n✅ All common columns have matching data types!")
    
    return missing_in_batch, missing_in_kafka, type_mismatches

def compare_data_samples(kafka_table, batch_table):
    """Compare sample data statistics."""
    print("\n" + "=" * 80)
    print("DATA SAMPLE COMPARISON")
    print("=" * 80)
    
    print(f"\n📈 Kafka Pipeline: {kafka_table.num_rows} rows")
    print(f"📈 Batch Pipeline: {batch_table.num_rows} rows")
    
    # Check common numeric columns
    common_cols = set(kafka_table.schema.names) & set(batch_table.schema.names)
    numeric_cols = ['open', 'high', 'low', 'close', 'total_volume_1m', 'trade_count_1m']
    
    print("\n📊 Sample Statistics for Key Columns:")
    for col in numeric_cols:
        if col in common_cols:
            kafka_data = kafka_table.column(col).to_pylist()
            batch_data = batch_table.column(col).to_pylist()
            
            kafka_min = min([x for x in kafka_data if x is not None])
            kafka_max = max([x for x in kafka_data if x is not None])
            batch_min = min([x for x in batch_data if x is not None])
            batch_max = max([x for x in batch_data if x is not None])
            
            print(f"\n   {col}:")
            print(f"      Kafka: min={kafka_min:.2f}, max={kafka_max:.2f}")
            print(f"      Batch: min={batch_min:.2f}, max={batch_max:.2f}")

def main():
    print("🔍 Validating Schema Parity: Kafka vs Batch Pipeline")
    print("=" * 80)
    
    # Get latest files
    print("\n📥 Fetching latest files from GCS...")
    
    try:
        # Get multiple Kafka files and merge them
        kafka_blobs = get_multiple_kafka_files("crypto-db-east1", "RealTime/year=2025/", num_files=5)
        print(f"✅ Kafka: Fetched {len(kafka_blobs)} recent files")
        for i, blob in enumerate(kafka_blobs[:3]):
            print(f"   [{i+1}] gs://crypto-db-east1/{blob.name}")
            print(f"       Created: {blob.time_created}, Size: {blob.size / 1024:.2f} KB")
    except Exception as e:
        print(f"❌ Error fetching Kafka files: {e}")
        sys.exit(1)
    
    try:
        batch_blob = get_latest_file("batch-btc-1h-east1", "")
        print(f"\n✅ Batch: gs://batch-btc-1h-east1/{batch_blob.name}")
        print(f"   Created: {batch_blob.time_created}")
        print(f"   Size: {batch_blob.size / 1024:.2f} KB")
    except Exception as e:
        print(f"❌ Error fetching Batch file: {e}")
        sys.exit(1)
    
    # Download and read parquet files
    print("\n📖 Reading Parquet files...")
    
    # Read and merge multiple Kafka files
    kafka_tables = []
    for blob in kafka_blobs:
        table = download_and_read_parquet("crypto-db-east1", blob.name)
        kafka_tables.append(table)
    
    # Concatenate all Kafka tables
    kafka_table = pa.concat_tables(kafka_tables)
    print(f"✅ Merged {len(kafka_tables)} Kafka files into {kafka_table.num_rows} rows")
    
    batch_table = download_and_read_parquet("batch-btc-1h-east1", batch_blob.name)
    print(f"✅ Read Batch file with {batch_table.num_rows} rows")
    
    # Compare schemas
    missing_in_batch, missing_in_kafka, type_mismatches = compare_schemas(kafka_table, batch_table)
    
    # Compare data samples
    compare_data_samples(kafka_table, batch_table)
    
    # Final verdict
    print("\n" + "=" * 80)
    print("FINAL VERDICT")
    print("=" * 80)
    
    if not missing_in_batch and not missing_in_kafka and not type_mismatches:
        print("✅ PERFECT PARITY: Schemas are identical!")
        sys.exit(0)
    elif not missing_in_batch and not type_mismatches:
        print("✅ SCHEMA PARITY: All Kafka features present in Batch pipeline")
        print("ℹ️  Batch has additional columns (acceptable)")
        sys.exit(0)
    else:
        print("❌ SCHEMA MISMATCH: Pipelines are not in parity")
        if missing_in_batch:
            print(f"   - {len(missing_in_batch)} columns missing in Batch pipeline")
        if type_mismatches:
            print(f"   - {len(type_mismatches)} type mismatches")
        sys.exit(1)

if __name__ == "__main__":
    main()
