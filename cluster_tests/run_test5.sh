#!/bin/bash

hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler0.warc.gz

zip pyfiles.zip *.py
export PYSPARK_PYTHON="/opt/anaconda3/bin/python"

spark-submit \
  --properties-file spark_properties.local1.conf \
  ArchiveProcessor.py \
    --input_warcs "example-20200623-crawler0.warc.gz" \
    --output_hbase

hdfs dfs -rm "example-20200623-crawler0.warc.gz"
