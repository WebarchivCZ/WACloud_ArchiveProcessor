#!/usr/bin/python
# coding: utf-8

"""..module:: archiveprocessor.LanguageIdentification.

..moduleauthor:: Jan Lehecka <jlehecka@ntis.zcu.cz>
"""
from typing import List, Optional, Dict
import os

from BaseAlgorithms import BaseAlgorithm
from config import STOPLISTS
from utils import strip_non_word_chars


class LanguageIdentifier(BaseAlgorithm):
    """Module to identify the most probable language of given text."""

    def _init(self) -> None:
        """Class constructor."""
        self.stoplists = self._load_stoplists()

    def guess_lang(
          self,
          text: str,
          min_sw_ratio: float = 0.05,
          min_sw_count: int = 3
          ) -> Optional[str]:
        """Guess the language of text based on stopwords counts.

        Args:
            text: Analyzed text.
            min_sw_ratio: Minimum ratio of stopwords in the text.
                This prevents incorrect guesses for unsupported languages.
            min_sw_count: Minimum number of stopwords in the text.
                This prevents incorrect guesses for too short texts.

        Returns:
            maxlang: The 2-char ISO 639-1 code of the most probable language.

        """
        words = text.split()
        words = [strip_non_word_chars(w) for w in words]
        words = [w.lower() for w in words if w]
        if not words:
            return None

        # at least this many stopwords must be present in the text
        min_count = max(min_sw_count, min_sw_ratio * len(words))

        maxN = 0.
        maxlang = None
        maxsw = []
        for lang, stoplist in self.stoplists.items():
            sw = [w for w in words if w in stoplist]
            N = len(sw)
            if N >= min_count and N > maxN:
                maxN = N
                maxlang = lang
                maxsw = sw

        self.logger.debug(
            f'Guessed lang={maxlang or "unk"}; found {maxN} stopwords '
            f'({maxN/len(words):.1%} of text): {set(maxsw)}.'
        )
        return maxlang

    def _load_stoplists(self) -> Dict[str, List]:
        """Load all stoplists.

        Returns:
            stoplists: Dictionary {lang: list of words}. Lang is 2-char
                ISO 639-1 code.

        """
        stoplists = {}
        for lang, fn in STOPLISTS.items():
            stoplists[lang] = self._load_stoplist(fn)
        return stoplists

    def _load_stoplist(self, path: str, encoding: str = "utf-8") -> List:
        """Load stopwords from a file.

        Args:
            path: Path to the source file with each word on one line. Empty
                lines are skipped.
            encoding: Encoding of the source file.

        Returns:
            stoplist: The loaded list of stopwords.

        Raises:
            ValueError if cannot read the source file.

        """
        stoplist = []
        try:
            text = open(path, 'r', encoding=encoding).read()
        except IOError as e:
            try:
                # try to search for the file in the root folder (spark
                # does not preserve folder structure when deploying)
                path_base = os.path.basename(path)
                text = open(path_base, 'r', encoding=encoding).read()
            except IOError:
                raise ValueError(f'Cannot load stoplist from {path} ({e})')
        for line in text.splitlines():
            word = line.strip()
            if not word:
                continue
            word = word.split()[0].lower()
            if word:
                stoplist.append(word)
        return stoplist
