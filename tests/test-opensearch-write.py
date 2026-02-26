#!/usr/bin/env python3
"""
Test opensearch-spark connector via Spark Connect.

Validates that the opensearch-spark-40 connector can write a DataFrame
to OpenSearch from the spark-connect-server. Run from the spark-runner pod:

    kubectl exec -it -n spark deployment/spark-runner -- bash
    export SPARK_REMOTE="sc://spark-connect.spark.svc.cluster.local:15002"
    python3 /path/to/test-opensearch-write.py

Prerequisites:
  - spark-connect-server running with opensearch-spark-40 in spark.jars.packages
  - OpenSearch cluster reachable at opensearch-cluster-aegis.opensearch.svc.cluster.local:9200
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

OPENSEARCH_HOST = "opensearch-cluster-aegis.opensearch.svc.cluster.local"
OPENSEARCH_PORT = "9200"
TEST_INDEX = "spark-connector-test"


def main():
    spark = SparkSession.builder.getOrCreate()

    print(f"Spark version: {spark.version}")
    print(f"Testing opensearch-spark connector write to {OPENSEARCH_HOST}:{OPENSEARCH_PORT}/{TEST_INDEX}")

    # Create a small test DataFrame
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("state", StringType(), True),
        StructField("score", FloatType(), True),
        StructField("count", IntegerType(), True),
    ])

    data = [
        ("test-001", "Alice Johnson", "TX", 0.95, 42),
        ("test-002", "Bob Smith", "CA", 0.87, 15),
        ("test-003", "Carol Davis", "NY", 0.91, 28),
        ("test-004", "Dave Wilson", "TX", 0.73, 8),
        ("test-005", "Eve Martinez", "FL", 0.88, 33),
    ]

    df = spark.createDataFrame(data, schema)
    print(f"Test DataFrame: {df.count()} rows")
    df.show()

    # Write to OpenSearch using the connector
    print(f"\nWriting to OpenSearch index '{TEST_INDEX}'...")
    try:
        (df.write
            .format("opensearch")
            .option("opensearch.nodes", OPENSEARCH_HOST)
            .option("opensearch.port", OPENSEARCH_PORT)
            .option("opensearch.net.ssl", "false")
            .option("opensearch.nodes.wan.only", "true")
            .option("opensearch.resource", TEST_INDEX)
            .option("opensearch.mapping.id", "id")
            .mode("overwrite")
            .save())
        print("Write successful!")
    except Exception as e:
        print(f"Write FAILED: {e}", file=sys.stderr)
        sys.exit(1)

    # Read back from OpenSearch to verify
    print(f"\nReading back from OpenSearch index '{TEST_INDEX}'...")
    try:
        df_read = (spark.read
            .format("opensearch")
            .option("opensearch.nodes", OPENSEARCH_HOST)
            .option("opensearch.port", OPENSEARCH_PORT)
            .option("opensearch.net.ssl", "false")
            .option("opensearch.nodes.wan.only", "true")
            .option("opensearch.resource", TEST_INDEX)
            .load())
        read_count = df_read.count()
        print(f"Read back {read_count} rows")
        df_read.show()

        if read_count == len(data):
            print("\nVALIDATION PASSED: Write and read-back successful")
        else:
            print(f"\nVALIDATION WARNING: Expected {len(data)} rows, got {read_count}")
    except Exception as e:
        print(f"Read-back FAILED: {e}", file=sys.stderr)
        print("(Write may still have succeeded — check OpenSearch directly)")
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()
