#!/usr/bin/python
# coding: utf-8

"""..module:: archiveprocessor.Tokenization.

..moduleauthor:: Jan Lehecka <jlehecka@ntis.zcu.cz>
"""
import nltk
import os

from BaseAlgorithms import BaseProcessAlgorithm
from metadata import LANGUAGE, URL, ID, PLAINTEXT, SENTENCES, TOKENS
from config import NLTK_DATA_DIR
from Record import Record

nltk.data.path = [NLTK_DATA_DIR]


class WordTokenizer(BaseProcessAlgorithm):
    """Tokenize plain text using NLTK package.

    If language of the text is not supported by NLTK or unknown, it is assumed
    to be Czech.

    Requires:
        NLTK package + Punkt corpus (included in this git):
            $ pip install nltk

    """

    def _init(self) -> None:
        """Class constructor."""
        self.lang_iso2punkt = {
            # language mapping from ISO 639-1 codes into Punkt corpus used
            # in NLTK, see:
            # https://github.com/joeyespo/gistmail/tree/master/nltk_data/tokenizers/punkt
            'cs': 'czech',
            'en': 'english',
            'de': 'german',
            'pl': 'polish',
            'fr': 'french',
            # sk and ru not supported at the time of writing
        }

    def _check_NLTK_data(self) -> None:
        """Move NLTK data into correct directory if necessary.

        NLTK data must be in a specific sub-directory, but Spark puts all files
        in the root of working directory.

        TODO: tell NLTK where files are without moving them?

        """
        dir_exists = os.path.isdir(NLTK_DATA_DIR)
        if not dir_exists and "czech.pickle" in os.listdir("."):
            self.logger.debug(f'Moving NLTK data.')
            dest = f"{NLTK_DATA_DIR}/tokenizers/punkt/PY3"
            os.makedirs(dest)
            for lang in self.lang_iso2punkt.values():
                os.rename(f"{lang}.pickle", f"{dest}/{lang}.pickle")

    def _process(self, record: Record) -> Record:
        """Process the record.

        Args:
            record: Record to be processed.

        Returns:
            record: Processed record.

        """
        lang = record[LANGUAGE] or 'cs'
        plang = self.lang_iso2punkt.get(lang, 'czech')
        text = record[PLAINTEXT] or ''
        self._check_NLTK_data()

        tokens = nltk.tokenize.word_tokenize(text, plang)
        record[TOKENS] = tokens

        self.logger.debug(
            f'Plain text split into {len(tokens)} tokens using '
            f'{plang.capitalize()} tokenizer (orig-lang={lang}, '
            f'URL {record[URL]} and ID="{record[ID]}").'
        )
        return record


class SentenceTokenizer(WordTokenizer):
    """Split plain text into sentences using NLTK package.

    If language of the text is not supported by NLTK or unknown, it is assumed
    to be Czech.

    Requires:
        Same as WordTokenizer.

    """

    def _process(self, record: Record) -> Record:
        """Process the record.

        Args:
            record: Record to be processed.

        Returns:
            record: Processed record.

        """
        lang = record[LANGUAGE] or 'cs'
        plang = self.lang_iso2punkt.get(lang, 'czech')
        text = record[PLAINTEXT] or ''
        self._check_NLTK_data()

        sentences = nltk.tokenize.sent_tokenize(text, plang)
        record[SENTENCES] = sentences

        self.logger.debug(
            f'Plain text split into {len(sentences)} sentences using '
            f'{plang.capitalize()} tokenizer (orig-lang={lang}, URL '
            f'{record[URL]} and ID="{record[ID]}").'
        )
        return record
