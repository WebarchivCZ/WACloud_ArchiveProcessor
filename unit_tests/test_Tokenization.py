# coding: utf-8
from unittest import mock
from assertpy import assert_that

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from Tokenization import WordTokenizer, SentenceTokenizer
from metadata import *
from Record import Record

def create_record(data={}):
    _data = {
        LANGUAGE: "cs",
        URL: "http://example.com",
        ID: "0",
    }
    _data.update(data)
    return Record(_data)

class TestTokenization():
    
    class TestWordTokenizer():
    
        def test_initialize(self):
            wt = WordTokenizer()
            assert_that(wt).is_instance_of(WordTokenizer)
            assert_that(wt.lang_iso2punkt).is_instance_of(dict)
            assert_that(wt.lang_iso2punkt).contains_key("cs", 'en', 'de','pl', 'fr')
        
        def test_process(self):
            wt = WordTokenizer()
            record = create_record({PLAINTEXT: "Dobrý den, toto je test."})
            ret = wt.process(record)
            assert_that(ret).is_instance_of(Record)
            assert_that(ret[TOKENS]).is_equal_to(["Dobrý", "den", ",", "toto", "je", "test", "."])
            
            record = create_record()
            ret = wt.process(record)
            assert_that(ret[TOKENS]).is_equal_to([])
        
            record = create_record({PLAINTEXT: "."})
            ret = wt.process(record)
            assert_that(ret[TOKENS]).is_equal_to(["."])
        
            record = create_record({PLAINTEXT: "První řádek.\nDruhý řádek."})
            ret = wt.process(record)
            assert_that(ret[TOKENS]).is_equal_to(["První", "řádek", ".", "Druhý", "řádek", "."])
            
        def test_process_langs(self):
            wt = WordTokenizer()
            
            record = create_record({PLAINTEXT: "This is a test.", LANGUAGE: "en"})
            assert_that(wt.lang_iso2punkt.get("en", 'czech')).is_equal_to("english")
            ret = wt.process(record)
            assert_that(ret[TOKENS]).is_equal_to(["This", "is", "a", "test", "."])
        
            record = create_record({PLAINTEXT: "Dies ist ein Test.", LANGUAGE: "de"})
            assert_that(wt.lang_iso2punkt.get("de", 'czech')).is_equal_to("german")
            ret = wt.process(record)
            assert_that(ret[TOKENS]).is_equal_to(["Dies", "ist", "ein", "Test", "."])
        
            record = create_record({PLAINTEXT: "To jest test.", LANGUAGE: "pl"})
            assert_that(wt.lang_iso2punkt.get("pl", 'czech')).is_equal_to("polish")
            ret = wt.process(record)
            assert_that(ret[TOKENS]).is_equal_to(["To", "jest", "test", "."])
            
            record = create_record({PLAINTEXT: "C'est un test.", LANGUAGE: "fr"})
            assert_that(wt.lang_iso2punkt.get("fr", 'czech')).is_equal_to("french")
            ret = wt.process(record)
            assert_that(ret[TOKENS]).is_equal_to(["C'est", "un", "test", "."])

        
    class TestSentenceTokenizer():
    
        def test_initialize(self):
            st = SentenceTokenizer()
            assert_that(st).is_instance_of(SentenceTokenizer)
            assert_that(st.lang_iso2punkt).is_instance_of(dict)
            assert_that(st.lang_iso2punkt).contains_key("cs", 'en', 'de','pl', 'fr')
        
        def test_process(self):
            wt = SentenceTokenizer()
            record = create_record({PLAINTEXT: "Dobrý den. Toto je test."})
            ret = wt.process(record)
            assert_that(ret).is_instance_of(Record)
            assert_that(ret[SENTENCES]).is_equal_to(["Dobrý den.", "Toto je test."])
            
            record = create_record({PLAINTEXT: "Přijdu 15.2.2023, tzn. v pondělí."})
            ret = wt.process(record)
            assert_that(ret).is_instance_of(Record)
            assert_that(len(ret[SENTENCES])).is_equal_to(1)
            
