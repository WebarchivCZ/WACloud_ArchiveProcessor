#!/bin/bash

hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler1.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler2.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler3.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler4.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler5.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler6.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler7.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler8.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler9.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler10.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler11.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler12.warc.gz
hdfs dfs -put ./example-20200623-crawler0.warc.gz example-20200623-crawler13.warc.gz

zip pyfiles.zip *.py
export PYSPARK_PYTHON="/opt/anaconda3/bin/python"

spark-submit \
  --properties-file spark_properties.local.conf \
  ArchiveProcessor.py \
    --input_warcs "example-20200623-crawler*.warc.gz" \
    --output_hbase

hdfs dfs -rm "example-20200623-crawler*.warc.gz"
