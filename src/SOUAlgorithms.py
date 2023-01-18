#!/usr/bin/python
# coding: utf-8

"""..module:: archiveprocessor.SOUAlgorithms.

..moduleauthor:: Jan Lehecka <jlehecka@ntis.zcu.cz>
"""
from typing import List, Optional
import re

import pyphen

from Tokenization import WordTokenizer, SentenceTokenizer
from BaseAlgorithms import BaseProcessAlgorithm
from Record import Record
from metadata import LANGUAGE, URL, ID, PLAINTEXT, TOKENS, SENTENCES, EXTRA


class FleschReadingEase(BaseProcessAlgorithm):
    """Compute the Flesch Reading Ease (FRE) score from plain text.

    To compute FRE, we need to know the number of sentences, words and
    syllables. We use following methods to split the text:
    - sentences are split from plain text using SentenceTokenizer (i.e. NLTK),
    - words are split from plain text using WordTokenizer (i.e. NLTK),
    - syllables are split from valid word tokens using hyphenation dictionaries
      from the pyphen package (https://pyphen.org/).

    """

    def _init(self) -> None:
        """Class constructor."""
        self.hyphenators = {
            "cs": pyphen.Pyphen(lang="cs"),
        }
        self.word_tokenizer = WordTokenizer()
        self.sentence_tokenizer = SentenceTokenizer()

    def check_lang_dict(self, record: Record) -> str:
        """Check hyphenation dictionary for given record.

        If language of the record is used for the first time, save the
        dictionary into cache. If hyphenation dictionary does not exists,
        fall back to Czech.

        Args:
            record: Analyzed record.

        Returns:
            lang: 2-char ISO 639-1 code of language.

        """
        lang = record[LANGUAGE] or 'cs'
        if lang not in self.hyphenators:
            try:
                lang_dic = pyphen.Pyphen(lang=lang)
            except KeyError:
                self.logger.warning(
                    f'No hyphenation dictionary for language {lang} '
                    f'(URL {record[URL]} and ID="{record[ID]}")'
                )
                lang = 'cs'
            else:
                self.hyphenators[lang] = lang_dic
        return lang

    def tokens_to_syllables(
          self,
          tokens: List[str],
          lang: str = "cs"
          ) -> List[str]:
        """Split given tokens (usually words) into syllables.

        Args:
            tokens: List of tokens.
            lang: ISO 639-1 code of language.

        Returns:
            syllables: List of syllables.

        """
        dic = self.hyphenators[lang]
        syllables = []
        for t in tokens:
            t_hyp = dic.inserted(t)
            # join multiple hyphens
            t_hyp = re.sub(r"\-+", "-", t_hyp)
            syllables += t_hyp.split("-")
        return syllables

    def FRE(
          self,
          N_sentences: int,
          N_words: int,
          N_syllables: int
          ) -> Optional[float]:
        """Compute the Flesch Reading Ease score.

        Args:
            N_sentences: Number of sentences.
            N_words: Number of words.
            N_syllables: Number of syllables.

        Returns:
            fre: Flesch reading ease score.

        """
        if N_sentences == 0 or N_words == 0:
            return None
        return 206.835 - 1.015 * (N_words / N_sentences) \
            - 84.6 * (N_syllables / N_words)

    def _process(self, record: Record) -> Record:
        """Process the record.

        Args:
            record: Record to be processed.

        Returns:
            record: Processed record.

        """
        text = record[PLAINTEXT]
        if not text:
            self.logger.debug(
                f'No plaintext (URL {record[URL]} and ID="{record[ID]}")'
            )
            record[EXTRA] += [record[URL], 0, 0, 0, None]
            return record

        lang = self.check_lang_dict(record)

        if TOKENS not in record.data:
            record = self.word_tokenizer.process(record)
        if SENTENCES not in record.data:
            record = self.sentence_tokenizer.process(record)

        # count only valid word tokens
        tokens = [t for t in record[TOKENS] if re.match(r"^[\w\-']+$", t)]
        syllables = self.tokens_to_syllables(tokens, lang)

        N_sentences = len(record[SENTENCES])
        N_words = len(tokens)
        N_syllables = len(syllables)

        fre = self.FRE(N_sentences, N_words, N_syllables)
        record[EXTRA] += [record[URL], N_sentences, N_words, N_syllables, fre]
        self.logger.debug(
            f'FleschReadingEase={fre} (URL {record[URL]} and '
            f'ID="{record[ID]}")'
        )
        return record
