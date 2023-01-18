#!/usr/bin/python
# coding: utf-8

"""..module:: archiveprocessor.config.

..moduleauthor:: Jan Lehecka <jlehecka@ntis.zcu.cz>
"""
from metadata import (
    CONTENT,
    TOKENS,
    SENTENCES,
    URLKEY,
    REFERSTO,
    HARVESTID,
    TITLE,
    PLAINTEXT,
    LANGUAGE,
    SENTIMENT,
    HEADLINES,
    TOPICS,
    LINKS,
    WEBPAGETYPE,
    RESPONSECODE
)


# Logger setting, possible levels are: debug, info, warning, error, critical.
# Set LOG_FILE to empty string for logging into STDOUT (works only in
# standalone local mode).
LOG_LVL = 'info'
LOG_FILE = '/opt/archiveprocessor/ArchiveProcessor.log'

# HBase settings
HBASE_HOST = ""
HBASE_PORT = 9099
HBASE_MAIN_TABLE = "main"
HBASE_HARV_TABLE = "harvest"
HBASE_CONF_TABLE = "config"
HBASE_PROC_TABLE = "processes"

PROC_STATUS_RUNNING = "running"
PROC_STATUS_FINISHED = "finished"
PROC_STATUS_FAILED = "failed"

# record types to be processed (this is 'WARC-Type' field in the WARC headers)
RECORD_TYPES = ["response", "revisit"]

# groups of supported MIME types (regular expressions to be matched)
RECORD_MIME_TYPES = {
    'HTML': r'(text/html|application/xhtml\+xml)',
    'PDF': r'application/pdf',
    'IMG': r'image/*',
    # TODO:audio/video
}

# process only records satisfying these metadata regexp filters
RECORD_FILTERS = {
    RESPONSECODE: r'^2\d\d$',
}

# HTML records larger than this size (in bytes) will be skipped.
# This is to prevent Justext from getting stucked for a long time on
# one extremely large HTML.
# Example of such record URL:
#    https://www.sportis.cz/search.php?rok=
#    (warning: your browser could be stucked for a while as well)
MAX_ALLOWED_HTML_CONTENT_SIZE = 20000000

# Skip WARC records with content length longer than this size (in bytes).
# This prevents memory problems with processing too long records with wrong
# MIME types (e.g. video saved as a text).
# In current project phase, we are interested only in records reasonable small
# (HTML, PDF, text, images, ...)
MAX_ALLOWED_WARC_CONTENT_SIZE = 100000000  # ~100 MB


JUSTEXT_BASE_SETTING = dict(
    length_low=70,
    length_high=140,
    stopwords_low=0.2,
    stopwords_high=0.3,
    max_link_density=0.4,
    max_good_distance=5,
    max_heading_distance=150,
    no_headings=True
)
# if JusText does not find any text, keep trying again with following setting
# fall-backs
JUSTEXT_FALLBACK_SETTING = [
    # allow whole text paragraph to be one link
    dict(max_link_density=1),
    # text paragraphs can be far away from each other
    dict(max_good_distance=20),
]

# stoplists are lists of the most frequent words in languages;
# they are used to guess what is the main language of a web page
STOPLISTS = {
    'cs': 'stoplists/cs.txt',
    'sk': 'stoplists/sk.txt',
    'en': 'stoplists/en.txt',
    'de': 'stoplists/de.txt',
    'pl': 'stoplists/pl.txt',
    'ru': 'stoplists/ru.txt',
    'fr': 'stoplists/fr.txt',
}

# metadata which will be dropped from the intermediary JSON before saving
UNNECESSARY_FIELDS = [
    CONTENT,
    TOKENS,
    SENTENCES
]

# metadata which will have separate column in output database.
# The output row for each record will have following columns:
#  key:         globally unique ID of the record
#  1,2,...,N-1: fields specified in OUTPUT_SEPARATE_COLS
#  N:           "IF": the rest of intermediary format (i.e. IF minus
#               UNNECESSARY_FIELDS, minus OUTPUT_SEPARATE_COLS)
OUTPUT_SEPARATE_COLS = [
    URLKEY,
    REFERSTO,
    HARVESTID,
    TITLE,
    PLAINTEXT,
    LANGUAGE,
    SENTIMENT,
    HEADLINES,
    TOPICS,
    LINKS,
    WEBPAGETYPE
]

# path to JSON schema of intermediary format
JSON_SCHEMA = 'schema.json'

# Directory where NLTK Punkt corpus is saved
NLTK_DATA_DIR = "NLTK_data"

# trained sklearn model for topic identification
TOPICS_CLF_MODEL = '/opt/archiveprocessor/TopicIdentification.pkl'

# trained sklearn model for sentiment analysis
SENTIMENT_CLF_MODEL = '/opt/archiveprocessor/SentimentAnalysis.pkl'
