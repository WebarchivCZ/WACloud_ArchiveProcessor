# coding: utf-8
from unittest import mock
from assertpy import assert_that
import pyphen

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from HBase import HBase
import happybase_mock

class TestHBase():
    @mock.patch('HBase.happybase')
    def test_initialize(self, mock_hb):
        hb = HBase("100.100.100.100", 1234)
        assert_that(hb).is_instance_of(HBase)
        assert_that(hb.host).is_equal_to("100.100.100.100")
        assert_that(hb.port).is_equal_to(1234)

    @mock.patch('HBase.happybase.Connection')
    def test_data(self, mock_hb):
        mock_hb.side_effect = happybase_mock.Connection
        hb = HBase("100.100.100.100", 1234)
        hb.check_table('my_table')
        ret = hb.put('my_table', 'rowkey', {'datakey': 'value'})
        
        assert_that(ret).is_true()
        assert_that(hb.get_tables()).contains(b'my_table')
        assert_that(hb.has_table('my_table')).is_true()
        assert_that(hb.has_table('other_table')).is_false()
        assert_that(hb.get_table('my_table')).is_instance_of(happybase_mock.Table)
        
        assert_that(hb.has_row('my_table', 'rowkey')).is_true()
        row = hb.get_row('my_table', 'rowkey')
        assert_that(row).is_instance_of(dict)
        assert_that(row).contains_key(b'cf1:datakey')
        assert_that(row[b'cf1:datakey']).is_equal_to(b'value')
        
        hb.delete_table('my_table')
        assert_that(hb.has_table('my_table')).is_false()
        assert_that(hb.get_tables()).is_equal_to(set())
        
    @mock.patch('HBase.happybase')
    def test_byte_conversion(self, mock_hb):
        hb = HBase("100.100.100.100", 1234)
        assert_that(hb.to_bytes(1)).is_equal_to(b"1")
        assert_that(hb.to_bytes(1.2)).is_equal_to(b"1.2")
        assert_that(hb.to_bytes("kulaťoučký kůň")).is_equal_to(b'kula\xc5\xa5ou\xc4\x8dk\xc3\xbd k\xc5\xaf\xc5\x88')
        assert_that(hb.to_bytes(["jedna", "dvě"])).is_equal_to(b'["jedna", "dv\\u011b"]')
        assert_that(hb.to_bytes({"key1": "val1", "key2": ["adf"], "key3": 15.9})).is_equal_to(
            b'8\x00\x00\x00\x02key1\x00\x05\x00\x00\x00val1\x00\x04key2\x00\x10\x00\x00\x00\x020\x00\x04\x00\x00\x00adf\x00\x00\x01key3\x00\xcd\xcc\xcc\xcc\xcc\xcc/@\x00'
        )
        
