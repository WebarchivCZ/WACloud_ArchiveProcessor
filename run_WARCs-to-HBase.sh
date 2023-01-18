#!/bin/bash

# Run full processing of given WARC collection and save results to HBase main table
#
# Args:
#    input_URI: URI to locate input collection of WARC files. Supported filesystems: 
#               local FS (URI "file:///path"), HDFS (URI "/path"). Globs are supported.
input_URI=$1

zip -j pyfiles.zip src/*.py
export PYSPARK_PYTHON="/opt/anaconda3/bin/python"

./scripts/check_HBase_ThriftServer.sh

spark-submit \
  --properties-file spark_properties/test_cluster.conf \
  src/ArchiveProcessor.py \
    --input_warcs $input_URI \
    --algseq "HTML:HTMLTextExtractor,WordTokenizer,TopicIdentifier,SentimentAnalyzer,WebPageTypeIdentifier" \
    --output_hbase
