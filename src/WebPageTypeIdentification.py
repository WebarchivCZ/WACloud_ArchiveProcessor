#!/usr/bin/python
# coding: utf-8

"""..module:: archiveprocessor.WebPageTypeIdentification.

..moduleauthor:: Jan Lehecka <jlehecka@ntis.zcu.cz>
"""
import json

from BaseAlgorithms import BaseProcessAlgorithm
from metadata import URL, ID, WEBPAGETYPE
from Record import Record


class WebPageTypeIdentifier(BaseProcessAlgorithm):
    """Predict the type of the web page."""

    # TODO: replace this dummy method by more sophisticated classifier
    def _init(self) -> None:
        """Class constructor."""
        self.NEWS_STRINGS = [
            "www.novinky.cz",
            "www.seznamzpravy.cz",
            "www.idnes.cz",
            "www.aktualne.cz",
            "www.denik.cz",
            "www.blesk.cz",
            "www.reflex.cz",
            "tn.nova.cz",
            "www.iprima.cz",
            "echo24.cz",
            "ct24.ceskatelevize.cz",
            "www.irozhlas.cz",
            "www.ceskenoviny.cz",
            "www.lidovky.cz",
            "www.forum24.cz",
            "ihned.cz",
            "www.parlamentnilisty.cz",
        ]
        self.ESHOP_STRINGS = [
            "www.alza.cz",
            "www.mall.cz",
            "eshop",
            "e-shop",
        ]
        self.FORUM_STRINGS = [
            "forum",
            "diskuse",
            "diskuze",
        ]

    def _process(self, record: Record) -> Record:
        """Process the record.

        Args:
            record: Record to be processed.

        Returns:
            record: Processed record.

        """
        url = record[URL]
        wpt = None
        for string in self.NEWS_STRINGS:
            if string in url:
                wpt = "news"
        if wpt is None:
            for string in self.ESHOP_STRINGS:
                if string in url:
                    wpt = "eshop"
        if wpt is None:
            for string in self.FORUM_STRINGS:
                if string in url:
                    wpt = "forum"
        if wpt is None:
            wpt = "others"
        record[WEBPAGETYPE] = wpt
        self.logger.debug(
            f'Detected web page type: {wpt} (URL {url} and '
            f'ID="{record[ID]}")'
        )
        return record

    def generate_webtypes_json(self, path: str) -> None:
        """Generate a JSON file with actual web page types.

        The JSON can be used to update HBase row in config table.

        Args:
            path: The path where to save the JSON file.

        """
        data = {"webtypes": {"value": {"eshop": 0, "news": 1, "forum": 2, "others": 3}}}
        json.dump(data, open(path, "w"), indent=2, ensure_ascii=False)
