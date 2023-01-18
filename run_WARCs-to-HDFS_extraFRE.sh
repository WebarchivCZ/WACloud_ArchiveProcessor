#!/bin/bash

# Run special processing of given WARC collection to save only FleschReadingEase
# output into a text file in HDFS
#
# Args:
#    input_URI: URI to locate input collection of WARC files. Supported filesystems: 
#               local FS (URI "file:///path"), HDFS (URI "/path"). Globs are supported.
#    output_URI: URI to write the output. Supported filesystems: 
#               local FS (URI "file:///path"), HDFS (URI "/path").
input_URI=$1
output_URI=$2

zip -j pyfiles.zip src/*.py
export PYSPARK_PYTHON="/opt/anaconda3/bin/python"

./scripts/check_HBase_ThriftServer.sh

spark-submit \
  --properties-file spark_properties/test_cluster_16CPUs.conf \
  src/ArchiveProcessor.py \
    --input_warcs $input_URI \
    --algseq "HTML:HTMLTextExtractor,FleschReadingEase" \
    --output_textfile_extra $output_URI
