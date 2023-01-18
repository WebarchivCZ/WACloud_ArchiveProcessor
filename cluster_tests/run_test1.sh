#!/bin/bash

# ======================== The simplest example ================================
# - extract plain texts from HTML records in example WARC file included in this 
#   git and save processed RDD into a text file output
# - this example does not require spark&hadoop&HBase installation nor any WARC 
#   collection and trained classifiers

export HOMEDIR="/opt/archiveprocessor_dev"
export PYSPARK_PYTHON="/opt/anaconda3/bin/python"
cd $HOMEDIR

zip -j pyfiles.zip src/*.py

spark-submit \
  --properties-file spark_properties/spark_properties.local1.conf \
  src/ArchiveProcessor.py \
    --input_warcs "file:///opt/archiveprocessor/example-20200623-crawler0.warc.gz" \
    --output_textfile_extra "file:///opt/archiveprocessor/output" \
    --algseq "HTML:HTMLTextExtractor" \

