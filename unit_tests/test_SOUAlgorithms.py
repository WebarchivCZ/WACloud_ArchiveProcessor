# coding: utf-8
from unittest import mock
from assertpy import assert_that
import pyphen

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from SOUAlgorithms import FleschReadingEase
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

class TestSOUAlgorithms():
    
    class TestFleschReadingEase():
    
        def test_initialize(self):

            fre = FleschReadingEase()
            assert_that(fre).is_instance_of(FleschReadingEase)
            assert_that(fre.hyphenators).contains_key("cs")
            assert_that(fre.hyphenators["cs"]).is_instance_of(pyphen.Pyphen)
            assert_that(fre.word_tokenizer).is_instance_of(WordTokenizer)
            assert_that(fre.sentence_tokenizer).is_instance_of(SentenceTokenizer)
        
        def test_check_lang_dict(self):
            fre = FleschReadingEase()
            record = create_record()
            ret = fre.check_lang_dict(record)
            assert_that(ret).is_equal_to("cs")
            
            record.data[LANGUAGE] = "en"
            ret = fre.check_lang_dict(record)
            assert_that(fre.hyphenators).contains_key("en")
            assert_that(ret).is_equal_to("en")
            
            # test fall-back language
            record[LANGUAGE] = "non-existing-lang"
            ret = fre.check_lang_dict(record)
            assert_that(ret).is_equal_to("cs")
            
        def test_tokens_to_syllables(self):
            fre = FleschReadingEase()
            ret = fre.tokens_to_syllables(["Dobrý", "den", "toto", "je", "test"])
            assert_that(ret).is_equal_to(["Dob", "rý", "den", "to", "to", "je", "test"])

            ret = fre.tokens_to_syllables(["nejneobhospodařovávatelnějšímu"])
            assert_that(ret).is_equal_to(['nej', 'ne', 'ob', 'hos', 'po', 'da', 'řo', 'vá', 'va', 'tel', 'něj', 'ší', 'mu'])

            ret = fre.tokens_to_syllables(["žluto-modrý", "kůň"])
            assert_that(ret).is_equal_to(['žlu', "to", "mod", "rý", "kůň"])

        def test_FRE(self):
            fre = FleschReadingEase()
            ret = fre.FRE(N_sentences=10, N_words=50, N_syllables=100)
            assert_that(ret).is_close_to(32.56, tolerance=1e-8)
        
        def test_process(self):
            fre = FleschReadingEase()
            record = create_record()
            ret = fre.process(record)
            assert_that(ret).is_instance_of(Record)
            assert_that(ret[EXTRA]).is_equal_to(["http://example.com", 0, 0, 0, None])
            
            record = create_record({PLAINTEXT: "Dobrý den. Toto je test."})
            ret = fre._process(record)
            assert_that(ret.data[EXTRA][:4]).is_equal_to(["http://example.com", 2, 5, 7])
            assert_that(ret[EXTRA][4]).is_close_to(85.8575, tolerance=1e-8)

            record = create_record({PLAINTEXT: "a"})
            ret = fre._process(record)
            assert_that(ret.data[EXTRA][:4]).is_equal_to(["http://example.com", 1, 1, 1])
            assert_that(ret[EXTRA][4]).is_close_to(121.22, tolerance=1e-8)

            record = create_record({PLAINTEXT: ","})
            ret = fre._process(record)
            assert_that(ret.data[EXTRA]).is_equal_to(["http://example.com", 1, 0, 0, None])

            record = create_record({PLAINTEXT: "10"})
            ret = fre._process(record)
            assert_that(ret.data[EXTRA][:4]).is_equal_to(["http://example.com", 1, 1, 1])
            assert_that(ret[EXTRA][4]).is_close_to(121.22, tolerance=1e-8)

            record = create_record({PLAINTEXT: "@ % + *"})
            ret = fre._process(record)
            assert_that(ret.data[EXTRA]).is_equal_to(["http://example.com", 1, 0, 0, None])

