#!/usr/bin/python
# coding: utf-8

"""..module:: archiveprocessor.ArchiveProcessor.

..moduleauthor:: Jan Lehecka <jlehecka@ntis.zcu.cz>
"""
from typing import Any, List, Optional, Iterator, Iterable, Tuple
from datetime import datetime
import traceback
import argparse
import copy
import json
import glob
import sys
import re

from jsonschema import validate
import pyspark
import pydoop.hdfs as hdfs
from pyspark.accumulators import AccumulatorParam
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from WebPageTypeIdentification import WebPageTypeIdentifier  # noqa: F401
from TopicIdentification import TopicIdentifier  # noqa: F401
from SentimentAnalysis import SentimentAnalyzer  # noqa: F401
from TextExtraction import HTMLTextExtractor, PDFTextExtractor  # noqa: F401
from BaseAlgorithms import BaseAlgorithm  # noqa: F401
from SOUAlgorithms import FleschReadingEase  # noqa: F401
from Tokenization import WordTokenizer, SentenceTokenizer  # noqa: F401
from Record import Record
from HBase import HBase
from utils import warc_name_to_harvest_info
from metadata import (
    ID,
    URL,
    EXTRA,
    MIMETYPE,
    RECHEADERS,
    WARCFILENAME,
    WARCOFFSET,
    HARVESTID
)
from config import (
    RECORD_TYPES,
    HBASE_HOST,
    HBASE_PORT,
    HBASE_MAIN_TABLE,
    HBASE_HARV_TABLE,
    HBASE_PROC_TABLE,
    PROC_STATUS_RUNNING,
    PROC_STATUS_FINISHED,
    PROC_STATUS_FAILED,
    OUTPUT_SEPARATE_COLS,
    JSON_SCHEMA,
    RECORD_MIME_TYPES,
    UNNECESSARY_FIELDS,
    RECORD_FILTERS,
    MAX_ALLOWED_WARC_CONTENT_SIZE
)


class ListAccumulatorParam(AccumulatorParam):
    """Extension of AccumulatorParam to lists.

    We use it to accumulate processed harvest IDs.

    """

    def zero(self, v: Any) -> List:
        """Provide a zero value for the type.

        Args:
            v: Any value.

        Returns:
            The zero value.

        """
        return []

    def addInPlace(self, variable: List, value: Any) -> List:
        """Add a value to the accumulator.

        Args:
            variable: Accumulator's variable.
            value: The new value.

        Returns:
            The updated variable.

        """
        variable.append(value)
        return variable


class ArchiveProcessor(BaseAlgorithm):
    """WebArchive Processor.

    This is the main module for processing WebArchive, generating intermediary
    records and saving processed data.

    For parallel computing, PySpark is used. Based on PySpark configuration,
    it can run locally on a single machine or on a HADOOP cluster.

    The intermediary format (IF) for each valid WARC record is a JSON fitting
    the JSON schema. When saving IF, unnecessary fields defined in `config.py`
    in `UNNECESSARY_FIELDS` are removed and fields defined in
    `OUTPUT_SEPARATE_COLS` are taken out to be saved in separate columns.

    Supported input formats of data are:
      - Collection of WARC files: WARC files are passed to the python driver
        as pyspark dataframes; each WARC is then read by one worker using
        warcio package and processed according to given arguments.
      - HBase main table (each row consisting of key and the record decomposed
        into `OUTPUT_SEPARATE_COLS` and the rest of IF.

    Supported output formats of data are:
      - HBase main table (each row consisting of key and the record decomposed
        into `OUTPUT_SEPARATE_COLS` and the rest of IF).
      - RDD as a text file (records decomposed in the same way as in HBase
        table).

    For usage, see --help output of this module.

    """

    def _init(
          self,
          input_warcs: str,
          input_hbase: bool,
          output_textfile: str,
          output_textfile_extra: str,
          output_hbase: bool,
          onlyIDs: Optional[str] = None,
          algseq: List[str] = []
          ) -> None:
        """Class constructor.

        Args:
            input_warcs: URI to locate input WARC files. Supported filesystems:
                - local FS (URI "file:///path/to/data"),
                - HDFS (URI "/path/to/data").
                Globs are supported.
            input_hbase: If True, data are read from the main HBase table
                (potential input_warcs argument is ignored). Skipping reading
                WARC files can be useful for fast updates in already processed
                data.
            output_textfile: URI to store output RDD as a text file. Supported
                filesystems:
                - local FS (URI "file:///path/to/data"),
                - HDFS (URI "/path/to/data").
            output_textfile_extra: URI to store extra RDD as a text file.
                Supported filesystems:
                - local FS (URI "file:///path/to/data"),
                - HDFS (URI "/path/to/data").
            output_hbase: If True, processed data are written into the main
                HBase table (potential output_textfile argument is ignored).
            onlyIDs: File with list of WARC record IDs to be processed. Useful
                to process only restricted subset of input data. The subset is
                defined by a file with list of WARC record IDs (one per line).
                Each ID should be UUID as URN (RFC 4122 standard), e.g.
                "urn:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
            algseq(list): Sequences of algorithms to process given data. Each
                sequence must be a string in format
                "record-type:alg1,alg2,...". Record types are groups of
                MIME-types defined in config.py (e.g. HTML, PDF, IMG, ...).
                Algorithms are names of processing classes
                (e.g. HTMLTextExtractor). Only record types specified in this
                argument will be processed and saved, all other will be
                ignored. For keeping record type in data without processing,
                set empty sequence of algorithms.

        """
        self.logger.info(f'Using python {sys.version} from {sys.executable}.')
        self.logger.info(
            f'Using pyspark=={pyspark.__version__} from {pyspark.__file__}.'
        )

        self.input_warcs = input_warcs
        self.input_hbase = input_hbase
        self.inputFormat = "hbase_table" if input_hbase else "warc_files"

        self.output_textfile = output_textfile
        self.output_textfile_extra = output_textfile_extra
        self.output_hbase = output_hbase
        if output_hbase:
            self.outputFormat = "hbase_table"
        elif output_textfile:
            self.outputFormat = "rdd_textFile"
        else:
            self.outputFormat = "rdd_textFile_extra"

        self.onlyIDs = onlyIDs
        self.algseq = algseq

        self.start_time = datetime.now()
        self.process_id = (
            f"ArchiveProcessor-{self.start_time:%Y-%m-%d-%H:%M:%S}"
        )
        t_started = int(datetime.timestamp(self.start_time) * 1000)
        self.process_info = {
            "t_started": t_started,  # HBase timestamp (in [ms])
            "cmd_args": " ".join(sys.argv),
            "harvests": [],
            "application_id": "",
        }

        self._load_JSON_schema()
        self._init_algorithms()
        self._load_onlyIDs()

        if self.output_hbase:
            self._check_HBase()

        self.output_col_names = OUTPUT_SEPARATE_COLS + ['IF']

    def _load_JSON_schema(self) -> None:
        """Read the JSON schema from file."""
        try:
            self.schema = json.load(open(JSON_SCHEMA, 'r'))
        except Exception as e:
            self.terminate(
                f'Cannot load JSON schema from {JSON_SCHEMA} ({e}).'
            )

    def _check_HBase(self) -> None:
        """Check the HBase tables. Create or enable them if necessary."""
        hb = HBase(HBASE_HOST, HBASE_PORT)
        hb.check_tables()
        hb.close()

    def _init_algorithms(self) -> None:
        """Initialize processing algorithms for all supported MIME types."""
        self.algorithms = {}
        if not self.algseq:
            return
        if not isinstance(self.algseq, list):
            self.algseq = [self.algseq]
        # set of all algorithms for all defined MIME types
        algs = set(a for mt, agsq in self.algseq for a in agsq)
        for alg in algs:
            self.algorithms[alg] = self._init_alg_or_terminate(alg)

    def _init_alg_or_terminate(self, alg_name: str, *args, **kwargs) -> Any:
        """Initialize algorithm specified by given name.

        Any additional arguments will be passed to the algorithm's constructor.

        Args:
            alg_name: The name of the algorithm.

        Returns:
            The instance of initialized algorithm.

        """
        try:
            # get class object
            cls = getattr(sys.modules[__name__], alg_name)
        except AttributeError:
            self.terminate(f'No imported algorithm with name "{alg_name}".')
        try:
            instance = cls(*args, **kwargs)
        except Exception as e:
            self.terminate(
                f'Algorithm "{alg_name}" cannot be initialized ({e}).'
            )
        return instance

    def _load_onlyIDs(self) -> None:
        """Load the set of record IDs to be processed."""
        self.processIDs = set()
        if self.onlyIDs is None:
            return
        try:
            for line in open(self.onlyIDs, 'r'):
                id = line.strip()
                if id:
                    self.processIDs.add(id)
            self.logger.info(
                f'Loaded {len(self.processIDs)} IDs from file {self.onlyIDs}.'
            )
        except Exception as e:
            self.terminate(f'Cannot load IDs from {self.onlyIDs}: {e}')

    def _update_proc_status(
          self,
          status: str,
          update_accumulators: bool = True
          ) -> None:
        """Update process info.

        When calling from inside a task, accumulators are not available and
        should not be accessed.

        Args:
            status: New status of the process.
            update_accumulators: Whether to update also accumulator variables.

        """
        self.process_info.update({
            "operation_status": status,
        })
        if status in [PROC_STATUS_FINISHED, PROC_STATUS_FAILED]:
            self.process_info.update({
                "t_finished": int(datetime.timestamp(datetime.now()) * 1000),
            })
        if update_accumulators and hasattr(self, "Nprocessed"):
            # each executor adds harvests into its own list,
            # the accumulator must be flattened and deduplicated
            harvests = set()
            for sublist in self.harvests.value:
                for item in sublist:
                    harvests.add(item)
            harvests = sorted(harvests)
            self.process_info.update({
                "records_processed": self.Nprocessed.value,
                "records_failed": self.Nfailed.value,
                "harvests": harvests,
            })
        if self.output_hbase:
            hb = HBase(HBASE_HOST, HBASE_PORT)
            hb.put(HBASE_PROC_TABLE, self.process_id, self.process_info)
            hb.close()

    def run(self) -> None:
        """Process data and save outputs."""
        self._update_proc_status(PROC_STATUS_RUNNING)
        try:
            rdd = self.process_data()
        except BaseException as e:
            self.terminate(f'Cannot process data: {e}')
        try:
            self.save_data(rdd)
        except BaseException as e:
            self.terminate(f'Cannot save data: {e}')
        self.logger.info(
            f'ArchiveProcessor ended ({self.Nprocessed.value} records'
            f' processed, {self.Nfailed.value} failed to process).'
        )
        self._update_proc_status(PROC_STATUS_FINISHED)

    def process_data(self) -> pyspark.rdd.RDD:
        """Choose the data processing method and process data.

        Returns:
            Processed data in the form of Resilient Distributed Datasets (RDD).

        """
        data_processor = self._get_data_processor()
        rdd = data_processor()
        return rdd

    def save_data(self, rdd: pyspark.rdd.RDD) -> None:
        """Choose the saving method and save outputs.

        Args:
            rdd: Processed data in the form of RDD.

        """
        data_saver = self._get_data_saver()
        data_saver(rdd)

    def _get_data_processor(self) -> Any:
        """Based on input arguments, choose the data processing method.

        Returns:
            Data processor.

        Raises:
            ValueError if the input format is not supported.

        """
        infor = self.inputFormat
        dp = getattr(self, f'process_{infor}', None)
        if dp is None:
            raise ValueError(f'Input format "{infor}" is not supported.')
        return dp

    def _get_data_saver(self):
        """Based on input arguments, choose the data saving method.

        Returns:
            Data saver.

        Raises:
            ValueError if the output format is not supported.

        """
        outfor = self.outputFormat
        ds = getattr(self, f'save_{outfor}', None)
        if ds is None:
            raise ValueError(f'Output format "{outfor}" is not supported.')
        return ds

    def process_warc_partition(
          self,
          _id: int,
          iterator: Any
          ) -> Iterator[Record]:
        """Process one data partition, i.e. one WARC file.

        Args:
            _id: Index of partition.
            iterator: Iterator with WARC file names; contains only one item.

        Returns:
            Generator over processed records.

        """
        for uri in iterator:
            if uri.startswith('file:/'):
                self.logger.info(f'Reading local stream {uri}')
                uri = re.sub(r"^file\:\/+", "/", uri)
                try:
                    stream = open(uri, 'rb')
                except IOError as e:
                    self.logger.error(f'Failed to open {uri}: {e}')
                    continue
            else:
                # HDFS
                self.logger.info(f"Reading from HDFS {uri}")
                try:
                    stream = hdfs.open(uri)
                except RuntimeError as e:
                    self.logger.error(f'Failed to open {uri}: {e}')
                    continue
            try:
                archive_iterator = ArchiveIterator(stream)
                for rec in archive_iterator:
                    rec_type = rec.rec_headers.get_header('WARC-Type')
                    if rec_type not in RECORD_TYPES:
                        continue
                    length = int(rec.rec_headers.get_header('Content-Length'))
                    if length > MAX_ALLOWED_WARC_CONTENT_SIZE:
                        continue
                    record = Record(rec, uri)
                    # !!! following command (getting offset) must take place
                    # AFTER reading content_stream (inside Record init),
                    # otherwise the content will be empty !!!
                    record[WARCOFFSET] = archive_iterator.get_record_offset()
                    record = self._process_record_catch_errors(record)
                    yield record
            except ArchiveLoadFailed as e:
                self.logger.error(f'Invalid WARC: {uri} - {e}')
            finally:
                stream.close()

    def process_warc_files(self) -> pyspark.rdd.RDD:
        """Set-up pySpark, initialize accumulators and process data as RDD.

        Returns:
            rdd: Processed data in the form of RDD.

        """
        sc = self.setup_pySpark()
        # sqlContext = pyspark.sql.SQLContext(sc)
        self.Nprocessed = sc.accumulator(0)
        self.Nfailed = sc.accumulator(0)
        self.harvests = sc.accumulator([], ListAccumulatorParam())

        if self.input_warcs.startswith('file:/'):
            # local FS
            uri = re.sub(r"^file\:\/+", "/", self.input_warcs)
            files = glob.glob(uri)
            files = [re.sub(r"^/", "file:///", fn) for fn in files]
        else:
            # HDFS; expand globs using JVM gateway
            URI = sc._gateway.jvm.java.net.URI
            Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
            FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
            Config = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

            fs = FileSystem.get(URI(self.input_warcs), Config())
            statuses = fs.globStatus(Path(self.input_warcs))
            files = [str(status.getPath()) for status in statuses]

        self.logger.info(f'Start processing {len(files)} WARC files.')

        # parallelize to N=len(files) partitions to process each WARC file by
        # one worker
        rdd = sc.parallelize(files, numSlices=len(files))

        # process data
        rdd = rdd.mapPartitionsWithIndex(self.process_warc_partition)

        # filter out empty records
        rdd = rdd.filter(lambda x: x is not None)
        return rdd

    def process_hbase_table(self) -> pyspark.rdd.RDD:
        """Set-up pySpark, get rows from HBase and process data as RDD.

        Returns:
            rdd: Processed data in the form of RDD.

        """
        raise NotImplementedError
        # return rdd

    def save_rdd_textFile_extra(self, rdd: pyspark.rdd.RDD) -> None:
        """Save RDD with extra data as a text file.

        Args:
            rdd: Processed data in the form of RDD.

        """
        rdd.saveAsTextFile(self.output_textfile_extra)

    def save_rdd_textFile(self, rdd: pyspark.rdd.RDD) -> None:
        """Save RDD with processed records as a text file.

        Args:
            rdd: Processed data in the form of RDD.

        """
        rdd.saveAsTextFile(self.output_textfile)

    def save_hbase_table(self, rdd: pyspark.rdd.RDD) -> None:
        """Save RDD with processed records into HBase table.

        Args:
            rdd: Processed data in the form of RDD.

        """
        rdd.foreachPartition(self._save_partition_happybase)

    def _process_HBase_row(self, row: Any) -> Record:
        """Process one row of HBase table.

        Args:
            row: HBase row.

        Returns:
            Processed record as a list of column values or None if processing
            fails.

        """
        record = Record(row)
        return self._process_record_catch_errors(record)

    def _process_record_catch_errors(
          self,
          record: Record
          ) -> Optional[List]:
        """Error-resistant record processing.

        Args:
            record: Unprocessed record.

        Returns:
            Processed record as a list of column values or None if processing
            fails.

        """
        try:
            return self.process_record(record)
        except Exception as e:
            self.logger.error(
                f'Error while processing record URL {record[URL]} and '
                f'ID="{record[ID]}": {e}\n{traceback.format_exc()}'
            )
            self.Nfailed += 1
            return None

    def process_record(self, record: Record) -> Optional[List]:
        """Process the record based on its MIME type.

        Args:
            record: Unprocessed record.

        Returns:
            Processed record as a list of column values, or None if either
            processing fails or the record does not match processing
            conditions.

        """
        if self.processIDs and record[ID] not in self.processIDs:
            self.logger.debug(
                f'Skipping record URL {record[URL]} and ID="{record[ID]}" (ID '
                f'not listed in {self.onlyIDs})'
            )
            return None

        try:
            self._record_check_filters(record)
        except ValueError as e:
            self.logger.debug(
                f'Skipping record URL {record[URL]} and ID="{record[ID]}" '
                f'({e})'
            )
            return None

        # validate record against JSON schema
        try:
            data_to_validate = dict(
                (k, v) for k, v in record.data.items() if k != EXTRA
            )
            validate(data_to_validate, self.schema)
        except Exception as e:
            self.logger.error(
                f'Skipping record URL {record[URL]} and ID="{record[ID]}" '
                f'(Invalid JSON: {e.message}).'
            )
            return None

        if self.outputFormat == "hbase_table":
            # check existence of corresponding row in HBASE_HARV_TABLE
            self._check_hbase_harvest_table(record)

        mime = record[MIMETYPE] or ''
        algseq = self._get_algseq_for_MIMEtype(mime)
        if algseq is None:
            # do not process MIME types out of --algseq input argument
            return None

        if record.is_revisit:
            # this is a revisit record without any content, no processing
            # necessary
            algseq = []

        algnames = [a.__class__.__name__ for a in algseq]
        for alg, name in zip(algseq, algnames):
            # modify only a deep copy of record for the possible case of
            # rollback
            new_record = alg.process(copy.deepcopy(record))
            # ensure the returned record is valid
            try:
                data_to_validate = dict(
                    (k, v) for k, v in new_record.data.items() if k != EXTRA
                )
                validate(data_to_validate, self.schema)
            except Exception as e:
                self.logger.error(
                    f'Ignoring returned data from algorithm "{name}", invalid '
                    f'JSON ({e.message}) (URL {record[URL]} and '
                    f'ID="{record[ID]}").'
                )
            else:
                record = new_record

        # record_type is e.g. response, revisit, ...
        record_type = record[RECHEADERS].get("WARC-Type", "")
        record = self._drop_unnecessary_fields(record)
        record_to_save = self.decompose_record(record)
        self.logger.info(
            f"Successfully processed record ID={record[ID]} "
            f"(TYPE={record_type}) with algorithms {algnames}; HBase row-key: "
            f"{record_to_save[0]}; origin: "
            f"{record[WARCFILENAME]}@{record[WARCOFFSET]}."
        )
        return record_to_save

    def _check_hbase_harvest_table(self, record: Record) -> None:
        """Check existence of corresponding row in HBASE_HARV_TABLE.

        Args:
            record: The record being processed.

        """
        hid = record[HARVESTID]
        if not hid or hid in self.process_info["harvests"]:
            return

        # check the HBase table
        hb = HBase(HBASE_HOST, HBASE_PORT)
        if not hb.has_row(HBASE_HARV_TABLE, hid):
            hmeta = warc_name_to_harvest_info(record[WARCFILENAME])
            data = {
                "type": hmeta.get("type", ""),
                "date": hmeta.get("date", ""),
            }
            self.logger.info(
                f"Found record from a new harvest: ID={hid}, {data}."
            )
            hb.put(HBASE_HARV_TABLE, hid, data)
        hb.close()
        self.harvests.add(hid)
        self._update_proc_status(
            PROC_STATUS_RUNNING,
            update_accumulators=False
        )

    def decompose_record(self, record: Record) -> List:
        """Convert Record object into output columns.

        Standard output columns are:
            [key] + OUTPUT_SEPARATE_COLS + [rest of IF]
        If the module is initialized with output_textfile_extra argument, the
        returned columns are:
            [key] + EXTRA

        Args:
            record: The Record object.

        Returns:
            List of values, one per column.

        """
        key = self._build_hbase_record_key(record)
        cols = [key]
        if self.outputFormat == "rdd_textFile_extra":
            cols += record[EXTRA]
        else:
            for field in OUTPUT_SEPARATE_COLS:
                val = record.data.pop(field, '')
                cols.append(val)
            cols.append(record.data)  # the rest of intermediary format
        return cols

    def _build_hbase_record_key(self, record: Record) -> str:
        """Generate the key for the HBase row for given record.

        Args:
            record: The record being processed.

        Returns:
            The HBase row key.

        """
        key = re.sub('^urn:uuid:', '', record[ID])
        return key

    def _get_algseq_for_MIMEtype(self, mime: str) -> Optional[List]:
        """Get sequence of processing algorithms for given MIME type.

        Args:
            mime: MIME type (e.g. "text/html")

        Returns:
            algseq: List of processing algorithms for given MIME type. Return
                None if this MIME type should not be processed.

        """
        if not hasattr(self, '_CACHEALGSEQ'):
            self._CACHEALGSEQ = {}

        # try to reuse cached sequence
        try:
            return self._CACHEALGSEQ[mime]
        except KeyError:
            pass

        # find sequence of algorithms for this MIME type and save it into cache
        for rtype, algnames in self.algseq:
            re_mime = RECORD_MIME_TYPES[rtype]
            if re.match(re_mime, mime, re.I):
                algseq = [self.algorithms[alg] for alg in algnames]
                self._CACHEALGSEQ[mime] = algseq
                return algseq

        # this MIME type should not be processed
        self._CACHEALGSEQ[mime] = None
        return None

    def _drop_unnecessary_fields(self, record: Record) -> Record:
        """Drop unnecessary fields before saving record.

        Args:
            record: The record being processed.

        Returns:
            The record with removed fields.

        """
        for field in UNNECESSARY_FIELDS:
            try:
                del record[field]
            except KeyError:
                pass
        return record

    def _record_check_filters(self, record: Record) -> None:
        """Check whether the record passes all defined metadata filters.

        Args:
            record: The record being processed.

        Raises:
            ValueError if the record violates any metadata filter.

        """
        for name, fltr in RECORD_FILTERS.items():
            val = record[name] or ''
            if not re.match(fltr, val):
                raise ValueError(
                    f'Value of {name} ({val}) did not pass filter {fltr}.'
                )

    def setup_pySpark(self) -> None:
        """Create SparkConf object and return initialized SparkContext.

        Returns:
            Spark context.

        """
        try:
            sc_conf = pyspark.SparkConf()
            sc = pyspark.SparkContext(conf=sc_conf)
        except Exception as e:
            self.terminate(f'Failed to initialize pyspark: {e}')
        self.logger.debug(f'pySpark context conf: {sc.getConf().getAll()}')
        self.process_info["application_id"] = sc._jsc.sc().applicationId()
        self._update_proc_status(PROC_STATUS_RUNNING)
        return sc

    def _save_partition_happybase(self, iter: Iterable) -> None:
        """Save RDD partition into HBase using happybase.

        Due to PySpark's serialization, connections cannot be created at the
        Spark driver and transferred across workers [1]. Instead, new
        connection must be created for each worker (i.e. for each RDD
        partition).

        Args:
            iter: Iterable with rows to be saved.

        References:  # noqa: E501
            [1] http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd

        """
        hb = HBase(HBASE_HOST, HBASE_PORT)
        for row in iter:
            row_key = row.pop(0)
            row_data = dict(zip(self.output_col_names, row))
            success = hb.put(HBASE_MAIN_TABLE, row_key, row_data)
            if success:
                self.Nprocessed += 1
            else:
                self.Nfailed += 1
        hb.close()

    def terminate(self, text=""):
        """Terminate the program in the case of any critical error.

        Args:
            text: Text to be logged along with the error.

        """
        self._update_proc_status(PROC_STATUS_FAILED)
        err = '' if not text else f": {text.rstrip('.')}"
        errtext = (
            f'Program has been terminated due to a critical error{err}. '
            f'Last traceback:\n{traceback.format_exc()}'
        )
        sys.stderr.write(errtext)
        self.logger.critical(errtext)
        sys.exit()


if __name__ == "__main__":

    def seq_alg(string: str) -> Tuple[str, List[str]]:
        """Validate input sequence of algorithms.

        Args:
            string: Input argument in the form "MIME-type-group:alg1,alg2,...".

        Returns:
            Tuple with 2 values: MIME type group and list of algorithms.

        """
        try:
            rtype, algs = string.split(':', 1)
        except ValueError:
            raise argparse.ArgumentTypeError(
                f'Cannot parse string "{string}", expected format is '
                f'"record-type:alg1,alg2,...".'
            )
        if rtype not in RECORD_MIME_TYPES:
            raise argparse.ArgumentTypeError(
                f'Record-type "{rtype}" is not supported.'
            )
        algs = algs.split(',') if algs else []
        return rtype, algs

    parser = argparse.ArgumentParser(
        description="WebArchive Processor",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    inpgpr = parser.add_mutually_exclusive_group(required=True)
    inpgpr.add_argument(
        '--input_warcs',
        metavar="URI",
        type=str,
        help='URI to locate input WARC files. Supported filesystems: '
        'local FS (URI "file:///path/to/data"), '
        'HDFS (URI "/path/to/data"). Globs are supported.'
    )
    inpgpr.add_argument(
        '--input_hbase',
        action='store_true',
        help='If true, data are read from the main HBase table. Skipping '
        'reading WARC files can be useful for fast updates in already '
        'processed data.'
    )

    outgrp = parser.add_mutually_exclusive_group(required=True)
    outgrp.add_argument(
        '--output_textfile',
        metavar="URI",
        type=str,
        help='URI to store output RDD as a text file. Supported filesystems: '
        'local FS (URI "file:///path/to/data"), '
        'HDFS (URI "/path/to/data").'
    )
    outgrp.add_argument(
        '--output_hbase',
        action='store_true',
        help='If true, processed data are written into the main HBase table.'
    )
    outgrp.add_argument(
        '--output_textfile_extra',
        metavar="URI",
        type=str,
        help='URI to save special RDD as a text file. Use this argument to '
        'export any special output structure or data not valid with the JSON '
        'schema. All data saved into "extra" field (list) during the '
        'processing will be exported. Supported filesystems: '
        'local FS (URI "file:///path/to/data"), '
        'HDFS (URI "/path/to/data").'
    )

    parser.add_argument(
        '--onlyIDs',
        metavar="file_name",
        type=str,
        help='Process only restricted subset of input data. The subset is '
        'defined by a file with list of WARC record IDs (one per line). Each '
        'ID should be UUID as URN (RFC 4122 standard), e.g. '
        '"urn:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6"'
    )
    parser.add_argument(
        '--algseq',
        metavar="rtype:alg1,alg2,...",
        default="HTML:HTMLTextExtractor",
        nargs='*',
        type=seq_alg,
        help='Sequences of algorithms to process given data. Each sequence '
        'must be a string in format "record-type:alg1,alg2,...". Record types '
        'are groups of MIME-types defined in config.py (e.g. HTML, PDF, IMG, '
        '...). Algorithms are names ''of processing clases (e.g. '
        'HTMLTextExtractor). Only record types specified in this argument '
        'will be processed and saved, all other will be ignored. For keeping '
        'record type in data without processing, set empty sequence of '
        'algorithms.'
    )

    args = parser.parse_args()
    ap = ArchiveProcessor(**vars(args))
    ap.run()
