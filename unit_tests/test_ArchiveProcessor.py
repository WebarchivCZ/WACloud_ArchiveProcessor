# coding: utf-8
from unittest import mock
from assertpy import assert_that
import pyphen

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from ArchiveProcessor import ArchiveProcessor

class TestArchiveProcessor():
    class TestArchiveProcessor():

        def test_initialize(self):
            ap = ArchiveProcessor(
                input_warcs="foo.warc",
                input_hbase=False,
                output_textfile="bar.txt",
                output_textfile_extra=None,
                output_hbase=False
            )
            assert_that(ap).is_instance_of(ArchiveProcessor)
