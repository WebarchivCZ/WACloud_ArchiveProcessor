# coding: utf-8
from unittest import mock
from assertpy import assert_that

import sys
import os
from io import BytesIO
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from metadata import *
from Record import Record
from warcio.statusandheaders import StatusAndHeaders
from warcio.recordbuilder import RecordBuilder
from warcio.recordloader import ArcWarcRecord

def create_ArcWarcRecord(payload, http_headers = [], warc_headers_dict={}, 
                         status='200 OK', wtype='response'):
    headers_list = [
        ('Content-Type', 'text/plain; charset="UTF-8"'),
        ('Content-Disposition', u'attachment; filename="испытание.txt"'),
        ('Custom-Header', 'somevalue'),
        ('Unicode-Header', '📁 text 🗄️'),
    ] + http_headers

    stat_http_headers = StatusAndHeaders(status, headers_list, protocol='HTTP/1.0')

    builder = RecordBuilder()
    return builder.create_warc_record(
        'http://example.com/',
        wtype,
        payload=BytesIO(payload),
        length=len(payload),
        http_headers=stat_http_headers,
        warc_headers_dict=warc_headers_dict
    )

def create_record(payload=b'some\ntext', http_headers = [], warc_headers_dict={}, 
    warc_file_name="test-20200623-crawler0.warc.gz", status='200 OK', wtype='response'):
    rec = Record(
        create_ArcWarcRecord(
            payload,
            http_headers = http_headers,
            warc_headers_dict = warc_headers_dict,
            status=status,
            wtype=wtype
        ),
        warc_file_name=warc_file_name
    )
    return rec

class TestRecord():
    
    def test_initialize_empty(self):
        rec = Record({})
        assert_that(rec).is_instance_of(Record)
        assert_that(rec.data).is_instance_of(dict)
        assert_that(rec.data).contains_key(EXTRA)
        assert_that(rec[EXTRA]).is_equal_to([])
    
    def test_initialize_dict(self):
        rec = Record({
            LANGUAGE: "cs",
            URL: "http://example.com",
            ID: "0",
        })
        assert_that(rec).is_instance_of(Record)
        assert_that(rec.data).is_instance_of(dict)
        assert_that(rec.data).contains_key(LANGUAGE, URL, ID, EXTRA)

    def test_initialize_warcio(self):
        warcio_rec = create_ArcWarcRecord(b'some\ntext')
        assert_that(warcio_rec).is_instance_of(ArcWarcRecord)
        assert_that(warcio_rec.content_stream().read()).is_equal_to(b'some\ntext')

        warcio_rec = create_ArcWarcRecord(b'some\ntext')
        rec = Record(warcio_rec, warc_file_name="test-20200623-crawler0.warc.gz")
        assert_that(rec).is_instance_of(Record)
        assert_that(rec.data).is_instance_of(dict)
        assert_that(rec.data).contains_key(
            CONTENT, ID, URLKEY, TIMESTAMP, URL, MIMETYPE, RESPONSECODE,
            DIGEST, RECHEADERS, HTTPHEADERS, HARVESTID, EXTRA
        )

    def test_ID(self):
        warcio_rec = create_ArcWarcRecord(
            b'some\ntext', 
            warc_headers_dict={
                'WARC-Record-ID': "<urn:uuid:fbd6cf0a-6160-4550-b343-12188dc05234>",
            }
        )
        rec = Record(
            warcio_rec, 
            warc_file_name="test-20200623-crawler0.warc.gz"
        )
        assert_that(rec[RECHEADERS]).contains_key('WARC-Record-ID')
        assert_that(rec[ID]).is_equal_to("urn:uuid:fbd6cf0a-6160-4550-b343-12188dc05234")

        del warcio_rec.rec_headers['WARC-Record-ID']
        rec = Record(
            warcio_rec, 
            warc_file_name="test-20200623-crawler0.warc.gz"
        )
        assert_that(rec[RECHEADERS]).does_not_contain_key('WARC-Record-ID')
        assert_that(rec.data).contains_key(ID)

    def test_content(self):
        rec = create_record(b'some\ntext')
        assert_that(rec[CONTENT]).is_equal_to("c29tZQp0ZXh0") # base64.b64encode(b'some\ntext')
        assert_that(rec.get_content_bytes()).is_equal_to(b'some\ntext')

    def test_content_gzip(self):
        rec = create_record(
            b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\n+\xce\xcfM\xe5*I\xad(\x01\x00\x1f\x8a\xcb\xc4\t\x00\x00\x00',
            http_headers = [('Content-Encoding', 'gzip')],
        )
        assert_that(rec.get_content_bytes()).is_equal_to(b'some\ntext')

    def test_content_deflate(self):
        rec = create_record(
            b'+\xce\xcfM\xe5*I\xad(\x01\x00',
            http_headers = [('Content-Encoding', 'deflate')],
        )
        # warcio should handle decompression during loading
        assert_that(rec[CONTENT]).is_equal_to("c29tZQp0ZXh0") # base64.b64encode(b'some\ntext')
        assert_that(rec.get_content_bytes()).is_equal_to(b'some\ntext')

    def test_content_brotli(self):
        rec = create_record(
            b'\x0b\x04\x80some\ntext\x03',
            http_headers = [('Content-Encoding', 'br')],
        )
        # warcio should handle decompression during loading
        assert_that(rec[CONTENT]).is_equal_to("c29tZQp0ZXh0") # base64.b64encode(b'some\ntext')
        assert_that(rec.get_content_bytes()).is_equal_to(b'some\ntext')

    def test_content_unk_compress(self):
        rec = create_record(
            b'some\ntext',
            http_headers = [('Content-Encoding', 'non-exist-compression')],
        )
        assert_that(rec[CONTENT]).is_equal_to("c29tZQp0ZXh0") # base64.b64encode(b'some\ntext')
        assert_that(rec.get_content_bytes()).is_equal_to(b'some\ntext')

    def test_revisit(self):
        rec = create_record(
            b'', 
            warc_headers_dict={
                'WARC-Refers-To': "<urn:uuid:fbd6cf0a-6160-4550-b343-12188dc05234>",
            }, 
            wtype='revisit',
        )
        assert_that(rec[RECHEADERS]).contains_key('WARC-Refers-To')
        assert_that(rec[REFERSTO]).is_equal_to('urn:uuid:fbd6cf0a-6160-4550-b343-12188dc05234')
        assert_that(rec.is_revisit).is_true()

    def test_redirect(self):
        rec = create_record(
            b'some\ntext', 
            http_headers=[
                ('Location', "/some/other/location"),
            ], 
            status='302 Found'
        )
        assert_that(rec[REDIRECT]).is_equal_to("/some/other/location")

    def test_check_int_fileds(self):
        rec = create_record(b'some\ntext')

        rec[WARCOFFSET] = 569874
        rec.check_int_fileds()
        assert_that(rec[WARCOFFSET]).is_equal_to(569874)

        rec[WARCOFFSET] = 569874.0
        rec.check_int_fileds()
        assert_that(rec[WARCOFFSET]).is_equal_to(569874)

        rec[WARCOFFSET] = "569874"
        rec.check_int_fileds()
        assert_that(rec[WARCOFFSET]).is_equal_to(569874)

        rec[WARCOFFSET] = "unsupported_type"
        rec.check_int_fileds()
        assert_that(rec.data).does_not_contain_key(WARCOFFSET)
