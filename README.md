# ArchiveProcessor

This is a module for processing WebArchive with machine learning algorithms and generating intermediary records.


For parallel computing, [PySpark](https://github.com/apache/spark/tree/master/python) is used. Based on [Spark configuration](http://spark.apache.org/docs/latest/configuration.html), it can 
run locally on a single machine or on a Hadoop cluster.

The module is implemented in Python, which offers large amount of easy-to-use packages for machine learning 
(e.g. data mining algorithms, supervised classifiers, deep neural networks, ...). These algorithms can be 
easily plugged in the WebArchive processing pipeline while following the 
Hadoop MapReduce philosophy.

The intermediary format (IF) for each valid WARC record is a JSON fitting 
    the [JSON schema](./schema.json). When saving IF, unnecessary fields defined in [config.py](./src/config.py)
    in `UNNECESSARY_FIELDS` are removed and fields defined in 
    `OUTPUT_SEPARATE_COLS` are taken out to be saved in separate columns.

***Supported input formats of data:***
* Collection of [WARC][1] files; records 
        are read using [warcio](https://github.com/webrecorder/warcio) package and converted into a pyspark dataframes; 
        each WARC file is processed by one 
        worker as one task.
* HBase main table (each row consisting of unique key and IF decomposed 
        into colums based on `OUTPUT_SEPARATE_COLS` setting.

***Supported output formats of data:***
* HBase main table (each row consisting of unique key and IF decomposed 
        into colums based on `OUTPUT_SEPARATE_COLS` setting).
* RDD as a text file (records decomposed in the same way as in HBase table).
* Special ad-hoc text file with custom data (export of any special output structure or
        data not valid with the JSON schema; see the `--output_textfile_extra` argument for details)

***Supported record types (defined in [config.py](./src/config.py)):***
*  `response` - the server response containing web page body
*  `revisit` - records indicating that web page was visited again but did not change from the last visit

***Supported MIME types (defined in [config.py](./src/config.py)):***
*  `HTML` - web pages (MIME types text/html and application/xhtml+xml)
*  ~~`PDF` - pdf documents (MIME type application/pdf)~~
*  ~~`IMG` - images (MIME types image/*)~~
*  ~~`AV` - audio and video formats~~

***Supported processing algorithms:***
*  [TextExtraction.py](./src/TextExtraction.py)
    * **HTMLTextExtractor** - extract plain text and other text-based metadata (links, title, headlines, language) from web pages (HTML)
    * ~~**PDFTextExtractor** - extract plain text and other text-based metadata (links, title, headlines, language) from PDF files~~
*  [Tokenization.py](./src/Tokenization.py)
    * **WordTokenizer** - split plain text into words (tokens)
    * **SentenceTokenizer** - split plain text into sentences
*  [TopicIdentification.py](./src/TopicIdentification.py)
    * **TopicIdentifier** - predict topics from plain text
*  [SentimentAnalysis.py](./src/SentimentAnalysis.py)
    * **SentimentAnalyzer** - predict sentiment from plain text
*  [WebPageTypeIdentification.py](./src/WebPageTypeIdentification.py)
    * **WebPageTypeIdentifier** - predict the type of web page (e.g. news/eshop/forum)
*  [SOUAlgorithms.py](./src/SOUAlgorithms.py)
    * **FleschReadingEase** - compute the Flesch Reading Ease (FRE) score from plain text

## Usage:
Tasks are started by the user "spark" (or another user via "sudo -u spark") by the command `spark-submit` (part of Spark) from the namenode from the directory `/opt/archiveprocessor`

For common data processing, scripts are prepared.

The progress of computation can be monitored on [ResourceManager UI](http://twann00.nkp.cz:8088/ui2) or with the command `yarn application -list'

### Use prepared script for standard tasks submission
* [run_WARCs-to-HBase.sh](./run_WARCs-to-HBase.sh) - processing a collection of WARCs (stored in the HDFS) and saving the results to HBase tables
  ```
  sudo -u spark ./run_WARCs-to-HBase.sh "/data/test_crawl/*.warc.gz"
  ```
* [run_WARCs-to-HDFS_extraFRE.sh](./run_WARCs-to-HDFS_extraFRE.sh) - processing a collection of WARCs (stored in the HDFS) and save FlashReadingEase into HDFS file
  ```
  sudo -u spark ./run_WARCs-to-HDFS_extraFRE.sh "/data/Volby2013/*.warc.gz" "/export/FRE_Volby2013"
  ```



### Or run custom data processing
<details><summary>Click to expand</summary>

##### 1. Edit selected spark properties configuration file in [spark_properties](./spark_properties) folder to set up Spark context
All spark-related setting ([cluster mode](http://spark.apache.org/docs/latest/submitting-applications.html#master-urls), 
memory setting, paths to AUT and other files, ...) should be defined here;
for full list of available Spark properties, see [Spark configuration page](http://spark.apache.org/docs/latest/configuration.html).
If not sure, select the one most suitable for you from the saved config files.

##### 2. Edit file [config.py](./src/config.py) 
You can optionally define how to set up processing algorithms, which MIME types are supported, logging setting etc.

##### 3. Submit [ArchiveProcessor.py](./src/ArchiveProcessor.py) with spark properties configuration file to Spark using `spark-submit` command with custom script:
  ```
  #!/bin/bash
  zip -j pyfiles.zip src/*.py
  export PYSPARK_PYTHON="/opt/anaconda3/bin/python"
  ./scripts/check_HBase_ThriftServer.sh
  spark-submit --properties-file spark_properties/test_cluster.conf src/ArchiveProcessor.py ...
  ```

Usage of *src/ArchiveProcessor.py*
```
usage: ArchiveProcessor.py [-h] (--input_warcs URI | --input_hbase)
                           (--output_textfile URI | --output_hbase | --output_textfile_extra URI)
                           [--onlyIDs file_name]
                           [--algseq [rtype:alg1,alg2,... [rtype:alg1,alg2,... ...]]]

WebArchive Processor

optional arguments:
  -h, --help            show this help message and exit
  --input_warcs URI     URI to locate input WARC files. Supported filesystems:
                        local FS (URI "file:///path/to/data"), HDFS (URI
                        "/path/to/data"). Globs are supported. (default: None)
  --input_hbase         If true, data are read from the main HBase table.
                        Skipping reading WARC files can be useful for fast
                        updates in already processed data. (default: False)
  --output_textfile URI
                        URI to store output RDD as a text file. Supported
                        filesystems: local FS (URI "file:///path/to/data"),
                        HDFS (URI "/path/to/data"). (default: None)
  --output_hbase        If true, processed data are written into the main
                        HBase table. (default: False)
  --output_textfile_extra URI
                        URI to save special RDD as a text file. Use this
                        argument to export any special output structure or
                        data not valid with the JSON schema. All data saved
                        into "extra" field (list) during the processing will
                        be exported. Supported filesystems: local FS (URI
                        "file:///path/to/data"), HDFS (URI "/path/to/data").
                        (default: None)
  --onlyIDs file_name   Process only restricted subset of input data. The
                        subset is defined by a file with list of WARC record
                        IDs (one per line). Each ID should be UUID as URN (RFC
                        4122 standard), e.g.
                        "urn:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
                        (default: None)
  --algseq [rtype:alg1,alg2,... [rtype:alg1,alg2,... ...]]
                        Sequences of algorithms to process given data. Each
                        sequence must be a string in format "record-
                        type:alg1,alg2,...". Record types are groups of MIME-
                        types defined in config.py (e.g. HTML, PDF, IMG, ...).
                        Algorithms are names of processing clases (e.g.
                        HTMLTextExtractor). Only record types specified in
                        this argument will be processed and saved, all other
                        will be ignored. For keeping record type in data
                        without processing, set empty sequence of algorithms.
                        (default: HTML:HTMLTextExtractor)
```

#### Examples:
To extract plain texts from HTML records in example WARC file included in this git and save processed RDD into a text file `test_output`, run:
```
spark-submit \
  --properties-file spark_properties.conf \
  src/ArchiveProcessor.py \
    --input_warcs "example-20200623-crawler0.warc.gz" \
    --output_textfile "test_output"
```

To extract plain texts, topics and sentiment from HTML records in all WARC files located in HDFS and save processed data into HBase table, run:
```
spark-submit \
  --properties-file spark_properties.conf \
  src/ArchiveProcessor.py \
    --input_warcs "hdfs://host:port/path/to/warc/files/*.warc.gz" \
    --algseq "HTML:HTMLTextExtractor,TopicIdentifier,SentimentAnalyzer" \
    --output_hbase
```
</details>

## Installation
<details><summary>Click to expand</summary>

*Note: This tool was optimized and tested for Hadoop Cluster with [Ambari 2.7.4.0](https://docs.cloudera.com/HDPDocuments/Ambari-2.7.4.0/bk_ambari-installation/content/ch_Getting_Ready.html) (with [HDP-3.1.4.0](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.4/release-notes/content/comp_versions.html)) on CentOS Linux release 7.6.1810.*

### 1. Preparing all machines in the cluster:
* *Run the following commands on all cluster machines (namenode + datanodes)*
* *Run it by any user with sudo rights and rights to gitlab repositories of ZČU*

**Required packages:**
```
sudo yum install -y git wget gcc-c++ java-1.8.0-openjdk-devel
```
**Install Anaconda (Python 3.7.6) into `/opt/anaconda3` (must be the same on all machines)**
```
cd ~
wget https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh -O ./anaconda.sh
chmod +x ./anaconda.sh
sudo ./anaconda.sh -b -p /opt/anaconda3
rm -f ./anaconda.sh
```
**Git clone + installation of necessary python packages:**
```
cd ~
git clone https://gitlab.nkp.cz/kybernetika-zcu/archiveprocessor.git
sudo /opt/anaconda3/bin/pip install -r ./archiveprocessor/requirements.txt
```
**Prepare folders and set permissions for local application storage (for local logs and models):**
```
sudo mkdir -p /opt/archiveprocessor
sudo setfacl -R -m u:spark:rwx,d:u:spark:rwx /opt/archiveprocessor
sudo setfacl -R -m u:yarn:rwx,d:u:yarn:rwx /opt/archiveprocessor
```

**Download trained classifiers for text analysis:**
* These are large files, versioning via gitlab is not suitable
* So far solved by a one-time (monthly expiration) link via filesender (CESNET)
```
cd /opt/archiveprocessor
sudo wget -O trained_classifiers.tar.gz "https://filesender.cesnet.cz/download.php?token=382475bb-da8d-cbf2-aca3-502b6e997043&files_ids=164942"
sudo tar xzf trained_classifiers.tar.gz
sudo rm trained_classifiers.tar.gz
```

### 2. Preparing the repository on the master namenode:
* *The following commands only need to be run on the machine from which the tasks will be run (namenode)*
* *Run it by any user with sudo rights and rights to gitlab repositories of ZČU*

**Copy the entire repository to `/opt/archiveprocessor` and set the rights of the users who will work with the tool:**
```
sudo cp -R ~/archiveprocessor /opt
sudo setfacl -R -m u:spark:rwx,d:u:spark:rwx /opt/archiveprocessor
sudo setfacl -R -m u:leheckaj:rwx,d:u:leheckaj:rwx /opt/archiveprocessor
...
```
</details>

## Tests:
<details><summary>Click to expand</summary>

### Test correctly installed Spark+YARN
* *Tests the short sample job included in the Spark installation in both deploy modes: YARN client and cluster*
* *Runs by user spark on namenode*
```
sudo -u spark spark-submit --master yarn --deploy-mode cluster --class org.apache.spark.examples.SparkPi /usr/hdp/3.1.4.0-315/spark2/examples/jars/spark-examples_2. 11-2.3.2.3.1.4.0-315.spring 10
sudo -u spark spark-submit --master yarn --deploy-mode client --class org.apache.spark.examples.SparkPi /usr/hdp/3.1.4.0-315/spark2/examples/jars/spark-examples_2. 11-2.3.2.3.1.4.0-315.spring 10
```

### Test the ArchiveProcessor tool
For the test, gradually run the tests in the directory [cluster_tests](./cluster_tests) from 1 (the simplest) to the full functionality of the tool in the cluster:

1. `sudo -u spark ./run_test1.sh` ... local extraction of the text of one WARC on local fs (after running, the directory "/opt/archiveprocessor/output" should appear with the processed data)
2. `sudo -u spark ./run_test2.sh` ... local extraction of the text of one WARC to HDFS (after running, the directory "/user/spark/output" should appear in HDFS)
3. `sudo -u spark ./run_test3.sh` ... local extraction of the text of one WARC into HBase (after running, records should appear in HBase tables
4. `sudo -u spark ./run_test4.sh` ... local extraction of text, topics and sentiment of one WARC into HBase (test whether the trained models work)
5. `sudo -u spark ./run_test5.sh` ... local copy of WARC to HDFS and text extraction of one WARC from HDFS to HBase
6. `sudo -u spark ./run_test6.sh` ... local text extraction of multiple WARCs from HDFS to HBase
7. `sudo -u spark ./run_test7.sh` ... local text extraction of multiple WARCs from HDFS to HBase, use all CPUs
8. `sudo -u spark ./run_test8.sh` ... YARN cluster: text extraction of multiple WARCs from HDFS to HBase
9. `sudo -u spark ./run_test9.sh` ... YARN cluster: extraction of text, topics and sentiment of more WARCs from HDFS to HBase (full functionality of the tool in the cluster)

### Unit tests
TBD
</details>

## Author:
Jan Lehečka, jlehecka@ntis.zcu.cz
    
    
[1]: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/
[2]: https://tools.ietf.org/html/rfc3548.html
