# coding: utf-8
from unittest import mock
from assertpy import assert_that

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from WebPageTypeIdentification import WebPageTypeIdentifier
from metadata import *
from Record import Record


class TestWebPageTypeIdentification():

    class TestWebPageTypeIdentifier():

        def test_initialize(self):

            wpti = WebPageTypeIdentifier()
            assert_that(wpti).is_instance_of(WebPageTypeIdentifier)

        def test_process(self):
            wpti = WebPageTypeIdentifier()
            record = Record({URL: "https://www.idnes.cz/zpravy/domaci/opici-nestovice-nemocnice-nakaza-cesko-svet.A220524_131640_domaci_ihal"})
            ret = wpti.process(record)
            assert_that(ret.data[WEBPAGETYPE]).is_equal_to("news")

            wpti = WebPageTypeIdentifier()
            record = Record({URL: "https://www.vari.cz/rady-a-navody/diskusni-forum/?thId=3681"})
            ret = wpti.process(record)
            assert_that(ret.data[WEBPAGETYPE]).is_equal_to("forum")

            wpti = WebPageTypeIdentifier()
            record = Record({URL: "https://www.alza.cz/siguro-ek-l24-advanced-glass-d7056775.htm"})
            ret = wpti.process(record)
            assert_that(ret.data[WEBPAGETYPE]).is_equal_to("eshop")


