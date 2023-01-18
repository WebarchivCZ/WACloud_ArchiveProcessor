#!/usr/bin/python
# coding: utf-8

"""..module:: archiveprocessor.metadata.

..moduleauthor:: Jan Lehecka <jlehecka@ntis.zcu.cz>
"""

# field names used in our JSON schema
ID = 'id'
CONTENT = 'content'
PLAINTEXT = 'plain-text'
TOKENS = 'plain-text-tokens'
SENTENCES = 'plain-text-sentences'
URLKEY = 'urlkey'
TIMESTAMP = 'timestamp'
URL = 'url'
MIMETYPE = 'mime-type'
RESPONSECODE = 'response-code'
DIGEST = 'digest'
REDIRECT = 'redirect-url'
ROBOTFLAGS = 'robot-meta-tags'
WARCOFFSET = 'warc-offset'
WARCSIZE = 'warc-record-size'
WARCFILENAME = 'warc-filename'
RECHEADERS = 'rec-headers'
HTTPHEADERS = 'http-headers'
TITLE = 'title'
HEADLINES = 'headlines'
LINKS = 'links'
LANGUAGE = 'language'
WEBPAGETYPE = 'web-page-type'
TOPICS = 'topics'
SENTIMENT = 'sentiment'
REFERSTO = 'refers-to'
HARVESTID = 'harvest-id'

# a special field for extra data not defined in JSON schema
EXTRA = 'extra'

# mapping from WARC record headers
WRH_META_MAP = {
    'WARC-Record-ID': ID,
    'WARC-Target-URI': URL,
    'Content-Length': WARCSIZE,
    'WARC-Date': TIMESTAMP,
    'WARC-Payload-Digest': DIGEST,
    'WARC-Refers-To': REFERSTO
}

INT_FIELDS = [WARCOFFSET, WARCSIZE]
