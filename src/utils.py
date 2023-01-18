#!/usr/bin/python
# coding: utf-8

"""..module:: archiveprocessor.utils.

..moduleauthor:: Jan Lehecka <jlehecka@ntis.zcu.cz>
"""
from typing import Dict, List, Optional, Union
import codecs
import base64
import re
import os


def warc_name_to_harvest_info(warc: str) -> Dict[str, str]:
    """Extract harvest info from the name of WARC file.

    Examples of WARC names:  # noqa: E501
    - MigrantCrisisEU-2015-02-20160220233522082-00275-8523~crawler00.webarchiv.cz~7778.warc.gz
    - CZ-2015-12-20151224071107633-01001-11606~crawler18.webarchiv.cz~7778.warc.gz
    - Test-2019-06-30-cc-naki-crawler00-20190701115137757-00008-12296~crawler00.webarchiv.cz~7778.warc.gz

    TODO: This is a very simple placeholder function to be updated based
        Grainery project (?)

    Args:
        warc: The filename or path of the WARC file.

    Returns:
        harvest_info: Metadata about the harvest the WARC came from.

    """
    harvest_info = {}
    warc = os.path.basename(warc)  # get filename
    m = re.match(r"(.+)\-(\d{8,})\-", warc)
    if m:
        name, tstamp = m.groups()
        htype = name.split('-')[0]
        harvest_info.update({
            'name': name,
            'type': htype,
            'date': tstamp[:8],
        })
    return harvest_info


def bytes_to_base64(byte_string: bytes) -> str:
    """Convert sequence of bytes into unicode string using base64.

    JSON serialization requires data as unicode strings, however some byte
    sequences are not valid in utf-8 (e.g. byte 0xff, i.e. binary 11111111
    is not allowed [1]), and so direct decoding to unicode can cause errors.

    To ensure the serialization of content will always work, base64
    algorithm [2] is used to encode any sequence of bytes into unicode
    string using only 64 ASCII chars.

    To decode base64-encoded content back into original byte sequence, run
    base64_to_bytes function.

    Args:
        byte_string: Sequence of bytes.

    Returns:
        ret_str: Sequence of ASCII chars.

    References:
        [1] https://en.wikipedia.org/wiki/UTF-8#Invalid_byte_sequences
        [2] https://tools.ietf.org/html/rfc3548.html

    """
    ret_str = base64.b64encode(byte_string)
    # the type of ret_str is still bytes
    ret_str = str(ret_str, 'utf-8')
    return ret_str


def base64_to_bytes(string: str) -> bytes:
    """Decode base64-encoded content back into original byte sequence.

    Args:
        string: Base64-encoded sequence of ASCII chars.

    Returns:
        byte_string: Sequence of bytes.

    """
    byte_string = base64.b64decode(string)
    return byte_string


def guess_charset(content: str, encodings: Optional[List[str]] = None) -> str:
    """Guess the charset of the content.

    The guess is based on selecting charset, which decodes the content
    with maximum ratio of Czech characters and minimum ratio of errors.
    This method should be called when there is no declaration about
    the encoding or to resolve encoding conflicts.

    Args:
        content: The content with unknown encoding.
        encodings: List of possible encodings to decide among.
            If `None` (default), several encodings typical for Czech
            documents will be used.
    Returns:
        charset: Guessed charset.

    Raises:
        ValueError if no content is given or if none of tested encodings
            decode the content.

    """
    if not content:
        raise ValueError(
            f'Cannot guess charset, the content is empty string.'
        )

    if encodings is None:
        encodings = [
            'ascii', 'utf-8', 'cp1250', 'iso-8859-2', 'cp1251', 'cp1252',
            'cp852', 'utf-16'
        ]
    czech_chars = u"aábcčdďeéěfghiíjklmnňoópqrřsštťuúůvwxyýzž" \
                  u"AÁBCČDĎEÉĚFGHIÍJKLMNŇOÓPQRŘSŠTŤUÚŮVWXYÝZŽ"

    content = content[:1000000]  # analyze only reasonable amount of text
    results = []

    for e in encodings:
        try:
            ucontent = content.decode(e, errors="replace")
        except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
            pass
        else:
            if len(ucontent) == 0:
                continue

            n_cz = sum(char in czech_chars for char in ucontent)
            cz_ratio = float(n_cz) / len(ucontent)

            n_er = ucontent.count(u"\uFFFD")  # count of replacement characters
            er_ratio = float(n_er) / len(ucontent)

            results.append((e, cz_ratio, er_ratio))

    if not results:
        raise ValueError(
            f'Cannot guess charset, none of {encodings} decoded the content.'
        )

    # sort by cz_ratio descending and secondary by er_ratio ascending
    results = sorted(results, key=lambda x: (-x[1], x[2]))

    charset, _, _ = results[0]
    return charset


def get_charset_from_BOM(text: bytes) -> Optional[str]:
    """Check if text starts with unicode byte order marks.

    Args:
        text: Analyzed byte string.

    Returns:
        charset: Guessed charset.

    """
    BOMS = {
        codecs.BOM_UTF8: 'utf-8-sig',
        codecs.BOM_UTF32_BE: 'utf-32-be',
        codecs.BOM_UTF32_LE: 'utf-32-le',
        codecs.BOM_UTF16_BE: 'utf-16-be',
        codecs.BOM_UTF16_LE: 'utf-16-le',
    }
    for bom, enc in BOMS.items():
        if text.startswith(bom):
            return enc
    return None


def known_encoding(encoding: str) -> bool:
    """Check if given encoding exists.

    Args:
        encoding: Tested encoding.

    Returns:
        Whether given encoding exists.

    """
    try:
        codecs.lookup(encoding)
    except LookupError:
        return False
    else:
        return True


def ensure_str(string: Union[str, bytes]) -> str:
    """Ensure the string is of type str, not bytes.

    Args:
        text: Analyzed string or bytes.

    Returns:
        Text as a string.

    """
    if isinstance(string, bytes):
        string = string.decode('utf-8', errors='replace')
    return string


def strip_non_word_chars(string: str) -> str:
    """Strip non-word characters from given string.

    All non-word characters at the beginning and end of given string are
    removed. All in-word characters are preserved no matter whether they
    are non-word or not.

    Args:
        string: Analyzed string.

    Returns:
        Stripped string.

    """
    return re.sub(r"(^[\W_]+)|([\W_]+$)", "", string)
