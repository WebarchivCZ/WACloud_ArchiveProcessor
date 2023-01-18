# coding: utf-8
from unittest import mock
from assertpy import assert_that

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from utils import *

class TestUtils():
    
    def test_warc_name_to_harvest_info(self):
        assert_that(warc_name_to_harvest_info("")).is_equal_to({})
        assert_that(warc_name_to_harvest_info("invalid/filename")).is_equal_to({})

        ret = warc_name_to_harvest_info(
            "MigrantCrisisEU-2015-02-20160220233522082-00275-8523~crawler00.webarchiv.cz~7778.warc.gz"
        )
        assert_that(ret["name"]).is_equal_to("MigrantCrisisEU-2015-02")
        assert_that(ret["type"]).is_equal_to("MigrantCrisisEU")
        assert_that(ret["date"]).is_equal_to("20160220")
        
        assert_that(warc_name_to_harvest_info(
            "/some/path/to/warc/" + "MigrantCrisisEU-2015-02-20160220233522082-00275-8523~crawler00.webarchiv.cz~7778.warc.gz"
        )).is_equal_to(ret)

        ret = warc_name_to_harvest_info(
            "CZ-2015-12-20151224071107633-01001-11606~crawler18.webarchiv.cz~7778.warc.gz"
        )
        assert_that(ret["name"]).is_equal_to("CZ-2015-12")
        assert_that(ret["type"]).is_equal_to("CZ")
        assert_that(ret["date"]).is_equal_to("20151224")
        
        ret = warc_name_to_harvest_info(
            "Test-2019-06-30-cc-naki-crawler00-20190701115137757-00008-12296~crawler00.webarchiv.cz~7778.warc.gz"
        )
        assert_that(ret["name"]).is_equal_to("Test-2019-06-30-cc-naki-crawler00")
        assert_that(ret["type"]).is_equal_to("Test")
        assert_that(ret["date"]).is_equal_to("20190701")
      
    def test_bytes_to_base64(self):
        ret = bytes_to_base64(b"Python is fun")
        assert_that(ret).is_equal_to("UHl0aG9uIGlzIGZ1bg==")
        assert_that(ret).is_instance_of(str)
        
        ret = bytes_to_base64(b"")
        assert_that(ret).is_equal_to("")
        assert_that(ret).is_instance_of(str)
    
    def test_base64_to_bytes(self):
        ret = base64_to_bytes("UHl0aG9uIGlzIGZ1bg==")
        assert_that(ret).is_instance_of(bytes)
        assert_that(ret).is_equal_to(b"Python is fun")
        
        ret = base64_to_bytes("")
        assert_that(ret).is_instance_of(bytes)
        assert_that(ret).is_equal_to(b"")

    def test_guess_charset(self):
        assert_that(guess_charset("Žluťoučký kůn".encode("utf-8"))).is_equal_to("utf-8")
        assert_that(guess_charset("Žluťoučký kůn".encode("cp1250"))).is_equal_to("cp1250")
        assert_that(guess_charset("Žluťoučký kůn".encode("iso-8859-2"))).is_equal_to("iso-8859-2")
        
        assert_that(guess_charset).raises(ValueError).when_called_with('')
        assert_that(guess_charset).raises(ValueError).when_called_with('A'.encode("utf-8"), encodings=["non-existing-enc"])
        
    def test_get_charset_from_BOM(self):
        assert_that(get_charset_from_BOM(b'text')).is_none()
        assert_that(get_charset_from_BOM(b'\xef\xbb\xbftext')).is_equal_to("utf-8-sig")
        assert_that(get_charset_from_BOM(b'\xfe\xfftext')).is_equal_to("utf-16-be")
        assert_that(get_charset_from_BOM(b'\xff\xfetext')).is_equal_to("utf-16-le")
        assert_that(get_charset_from_BOM(b'\x00\x00\xfe\xfftext')).is_equal_to("utf-32-be")
        assert_that(get_charset_from_BOM(b'\xff\xfe\x00\x00text')).is_equal_to("utf-32-le")
        
    def test_known_encoding(self):
        assert_that(known_encoding('cp1250')).is_true()
        assert_that(known_encoding('made-up-enc')).is_false()
        
    def test_ensure_str(self):
        assert_that(ensure_str('text')).is_equal_to('text')
        assert_that(ensure_str(b'text')).is_equal_to('text')

    def test_strip_non_word_chars(self):
        assert_that(strip_non_word_chars('.-%text!>_')).is_equal_to('text')
        assert_that(strip_non_word_chars('te_*xt*')).is_equal_to('te_*xt')
        assert_that(strip_non_word_chars('*text12*')).is_equal_to('text12')

        
    
