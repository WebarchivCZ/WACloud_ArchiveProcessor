#!/usr/bin/env python3
# coding: utf-8
from __future__ import print_function
from collections import defaultdict
import codecs
import wget
import gzip

# extracted plain texts from wikipedia dumps:
# https://lindat.mff.cuni.cz/repository/xmlui/handle/11234/1-2735
TEXT_URLS = {
    'cs': 'https://lindat.mff.cuni.cz/repository/xmlui/bitstream/handle/11234/1-2735/cs.txt.gz?sequence=54&isAllowed=y',
    'en': 'https://lindat.mff.cuni.cz/repository/xmlui/bitstream/handle/11234/1-2735/en.txt.gz?sequence=73&isAllowed=y',
    'de': 'https://lindat.mff.cuni.cz/repository/xmlui/bitstream/handle/11234/1-2735/de.txt.gz?sequence=74&isAllowed=y',
    'fr': 'https://lindat.mff.cuni.cz/repository/xmlui/bitstream/handle/11234/1-2735/fr.txt.gz?sequence=85&isAllowed=y',
    'pl': 'https://lindat.mff.cuni.cz/repository/xmlui/bitstream/handle/11234/1-2735/pl.txt.gz?sequence=208&isAllowed=y',
    'sk': 'https://lindat.mff.cuni.cz/repository/xmlui/bitstream/handle/11234/1-2735/sk.txt.gz?sequence=240&isAllowed=y',
    'ru': 'https://lindat.mff.cuni.cz/repository/xmlui/bitstream/handle/11234/1-2735/ru.txt.gz?sequence=227&isAllowed=y',
    }

# frequent, but confusing words in corpora
IGNORED = {
    'cs': ['and', 'of', 'the', 'de', 'in', 'von', 'ii', 'n', 'c', 'mm', 'cm', 
           'b', 'x', 'iii', 'd', 'j', 'fc', 'p', 'e', 't', 'f', 'iv', 'g', 'kg',
           'l', 'm', 'km', 'r', 'usa', 'open', 'la', u'km²'],
    'sk': ['and', 'of', 'the', 'de', 'in', 'von', 'im', 'm', 'ii', 'r', 'c', 'n', 
           'j', 'b', 'york', 'iii', 't', 'x', 'd', 'g', 'e', 'f', 'l', 'iv', 'p',
           'km', 'usa', 'open', 'la', u'km²', 'cm', 'h', 'mm'],
    'en': ['de', 'festival', 'ii', 'm', 'c', 'b', 'd', 'v', 'e', 'km', 'j', 'p',
           'f', 'x', 'r', 's', 'la', 'york'],
    'de': ['and', 'of', 'the', 'm', 'b', 'km', 'i', 'ii', 'z', 'v', 'u',
           'c', 'fc', 'd', 's', 'e', 'iii', 'mm', 'h', 'cm', 'n', 'kg', 'usa',
           'la', u'km²', 'york', 't'],
    'pl': ['and', 'of', 'the', 'de', 'in', 'von', 'im', 'ii', 'm', 'km', 'iii', 
           'xx', 'xix', 'mm', 'b', 'c', 'iv', 'cm', 'x', 'fc', 'kg', 'e', 'l', 
           't', 'd', 'j', 'p', 'g', 'xvii', 'r', 'usa', 'la', u'km²', 'f', 
           'xviii', 'ok'],
    'ru': ['and', 'of', 'the', 'i', 'ii', 'xx', 'p', 'a', 'iv', 'v', 'xviii', 
           'c', 'xix', 'iii'],
    'fr': ['and', 'of', 'the', 'von', 'festival', 'm', 'ii', 'd', 'b', 'iii', 'e',
           's', 'i', 'c', 'l', 'n', 'h', 'v', 'x', 'fc', 'km', 'rock', 'york'],
    }

BUFFER_SIZE = 500000 # N words to be precessed at once
STRIPCHARS = u""".,:;-+!?'"_@()[]<>/–1234567890%•=*°×†&„—·−#$»«"""

def get_stopwords(lang, Ntop):
    url = TEXT_URLS.get(lang)
    if not url:
        raise ValueError('Unsupported language "%s"' % lang)

    vocab = defaultdict(int)
    N_words = 0

    print('\nDownloading texts from %s' % url)
    fn = wget.download(url)
    #fn = '%s.txt.gz' % lang
    print('\nProcessing %s...' % fn)
    buff_words = []
    ign = IGNORED.get(lang, [])
    with gzip.open(fn, 'rt', encoding='utf-8') as fr:
        for i, line in enumerate(fr):
            words = line.split()
            words = [w.strip(STRIPCHARS) for w in words]
            words = [w.lower() for w in words if w]
            buff_words += [w for w in words if w not in ign]
            if len(buff_words) >= BUFFER_SIZE:
                for w in buff_words:
                    vocab[w] += 1
                N_words += len(buff_words)
                buff_words = []
                print('%d lines done, word count=%d, vocab size=%d' % (
                    i, N_words, len(vocab)))
        if buff_words:
            for w in buff_words:
                vocab[w] += 1
            N_words += len(buff_words)
            print('%d lines done, word count=%d, vocab size=%d' % (
                i, N_words, len(vocab)))

    stopwords = sorted(vocab.items(), key = lambda x:x[1], reverse=True)[:Ntop]
    sw_count = sum(c for w,c in stopwords)
    perc = float(sw_count) * 100 / N_words
    print('%d most frequent words have total count %d (%.2f%% of the corpus)' % (
        Ntop, sw_count, perc))
    print('Saving stopwords into %s.txt' % lang)
    with codecs.open('%s.txt'%lang, 'w', 'utf-8') as fw:
        for w, count in stopwords:
            print('%s\t%d' % (w, count), file=fw)

if __name__== "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description = "Stopwords downloader",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('-l', metavar='langs', type=str, help='Comma-separated '
                        'languages in ISO 639-1 code', 
                        default=",".join(TEXT_URLS.keys()))
    parser.add_argument('-n', metavar='Ntop', type=int, default=1000, 
                        help='Number of saved stopwords')
    args = parser.parse_args()
    for lang in args.l.split(','):
        get_stopwords(lang, args.n)
