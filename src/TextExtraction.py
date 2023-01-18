# coding: utf-8
from html.parser import HTMLParser
from lxml.html.clean import Cleaner
from cgi import parse_header
import urllib.parse
import lxml.etree
import lxml.html
import justext
import sys
import re

from utils import guess_charset, get_charset_from_BOM, known_encoding
from LanguageIdentification import LanguageIdentifier
from BaseAlgorithms import BaseProcessAlgorithm
from metadata import *
from config import *

class HTMLTextExtractor(BaseProcessAlgorithm):
    """ Get plain text from the HTML.
    
    The plain text is extracted using JusText tool 3.0.
    
    This module also deals with extractions of text-based metadata (links, 
    title, headlines, language). 
    
    """
    
    def _init(self):
        self.guess_lang = LanguageIdentifier().guess_lang
        self.unescape = HTMLParser().unescape

        jv = justext.__version__
        if jv != '3.0':
            raise ImportError(f'jusText 3.0 expected, but found version {jv}')
        # languages supported by jusText
        self.jstoplists = {
            'cs': justext.get_stoplist('Czech'),
            'sk': justext.get_stoplist('Slovak'),
            'en': justext.get_stoplist('English'),
            'de': justext.get_stoplist('German'),
            'pl': justext.get_stoplist('Polish'),
            'ru': justext.get_stoplist('Russian'),
            'fr': justext.get_stoplist('French'),
            }

        # base HTML cleaner to get all text from the web page
        self.cleaner = Cleaner()
        # activate tag filters (True means elements will be removed)
        self.cleaner.javascript = True
        self.cleaner.scripts = True
        self.cleaner.style = True

    def _process(self, data):
        
        # get HTML
        html = data.get_content_bytes()
        if not html.strip():
            self.logger.debug(f'No html (URL {data[URL]} and ID="{data[ID]}")')
            return data

        if len(html) > MAX_ALLOWED_HTML_CONTENT_SIZE:
            self.logger.warning(f'Too large HTML ({len(html)} bytes), skipping '
                f'record (URL {data[URL]} and ID="{data[ID]}")')
            return data

        # decode HTML
        charset, chtype = self._get_charset(data, html)
        uhtml = html.decode(charset, errors='replace')
        n_errors = uhtml.count(u"\uFFFD") # count replacement characters
        if n_errors > 0:
            self.logger.warning(f'Found {n_errors} errors while decoding '
                f'content from {chtype} charset {charset}, (URL {data[URL]} '
                f'and ID="{data[ID]}")')

        # remove XHTML declaration (otherwise lxml parser raises exception)
        uhtml = re.sub('^\s*<\?xml[^>]*>\s*', '', uhtml, flags = re.S|re.I)
      
        # parse HTML
        try:
            tree = lxml.html.fromstring(uhtml)
        except lxml.etree.ParserError as e:
            if str(e) == "Document is empty":
                self.logger.debug(f'Document is empty (URL {data[URL]} '
                    f'and ID="{data[ID]}")')
                return data
            else:
                raise e

        # extract metadata from HTML
        try:
            metadata = self._get_metadata(tree)
        except Exception as e:
            self.logger.warning(f'Error while getting metadata: {e} (URL '
                f'{data[URL]} and ID="{data[ID]}").')
        else:
            data.data.update(metadata)

        # extract links from HTML
        try:
            data[LINKS] = self._get_abs_links(tree, data[URL])
        except Exception as e:
            self.logger.warning(f'Error while getting links: {e} (URL '
                f'{data[URL]} and ID="{data[ID]}").')

        # get language-specific stoplist
        lang = data[LANGUAGE] or 'cs'
        if lang not in self.jstoplists:
            self.logger.warning(f'Language %s is not supported (URL %s and ID="%s")' % (
                lang, data[URL], data[ID]))
            return data
        stoplist = self.jstoplists[lang]

        # extract plain text
        paragraphs = self._justext(html, stoplist, charset, JUSTEXT_BASE_SETTING)
        if not paragraphs:
            for upd in JUSTEXT_FALLBACK_SETTING:
                self.logger.debug(f'JusText did not find any text, trying '
                    f'again with {upd} (URL {data[URL]} and ID="{data[ID]}")')
                setting = JUSTEXT_BASE_SETTING.copy()
                setting.update(upd)
                paragraphs = self._justext(html, stoplist, charset, setting)
                if paragraphs:
                    break
        text = '\n'.join(p['text'] for p in paragraphs)
        text = text.strip()

        self.logger.debug(f'Plain text: {len(paragraphs)} paragraphs, '
            f'{len(text.split())} words and {len(text)} chars ({chtype} '
            f'charset={charset}, lang={data[LANGUAGE] or "unk"}, '
            f'title="{data[TITLE]}", {len(data[HEADLINES] or [])} headlines, '
            f'{len(data[LINKS] or [])} links, URL {data[URL]} and '
            f'ID="{data[ID]}").')
        data[PLAINTEXT] = text
        return data

    def _justext(self, html, stoplist, charset, jsetting):
        """ Get list of paragraphs with positive classification. """
        paragraphs = justext.justext(
            html,
            stoplist = stoplist,
            encoding = charset,
            **jsetting
        )
        # select only paragraphs classified as "good"
        paragraphs = [p for p in paragraphs if p['class'] == 'good']
        return paragraphs

    def _get_metadata(self, tree):
        """ Extract metadata (title, headlines etc.) from parsed HTML. """
        metadata = {}

        def norm_text(text):
            text = str(text)
            text = self.unescape(text)
            text = re.sub('\s+', ' ', text).strip()
            return text

        lang = self._guess_lang_html(tree)
        if lang:
            metadata[LANGUAGE] = lang

        title = tree.xpath('//title/text()')
        if title:
            metadata[TITLE] = norm_text(title[0])

        headlines = []
        for level in range(1,7):
            headlines += tree.xpath('//h%d/text()' % level)
        headlines = list(map(norm_text, headlines))
        metadata[HEADLINES] = [h for h in headlines if h]

        return metadata

    def _get_abs_links(self, tree, base_url=''):
        """ Get all links (as absolute URLs) from parsed HTML. """
        links = tree.xpath('//a/@href')
        links = [l for l in links if l]
        if base_url:
            links = [urllib.parse.urljoin(base_url, l) for l in links]
        links = list(filter(self._valid_abs_url, links))
        return links

    def _valid_abs_url(self, url):
        """ Check if given string is valid absolute URL. """
        try:
            result = urllib.parse.urlparse(url)
            # absolute URLs must contain scheme and netloc
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def _guess_lang_html(self, tree):
        """ Guess the language of parsed web page based on stopword counts. """
        # get all words visible on the web page
        ctree = self.cleaner.clean_html(tree)
        text = ctree.text_content()
        return self.guess_lang(text)

    def _get_charset(self, data, html):
        """ Decide about character set used in the HTML. """ 
        charsets = set()

        # first, check BOM, if found, do not search further
        charset = get_charset_from_BOM(html)
        if charset is not None and known_encoding(charset):
            return charset, 'BOM-based'

        # look into the header
        ct = (data[HTTPHEADERS] or {}).get('Content-Type', '')
        mimetype, params = parse_header(ct)
        charset = params.get('charset', None)
        if charset is not None and known_encoding(charset):
            charsets.add(charset.lower())

        # look into meta tag of the web page
        patterns = [
            """<meta\s+[^>]*?charset\s*=[\s"']*([^\s"'/>]+)""", # HTML
            """^\s*<\?xml\s+[^>]*?encoding\s*=[\s"']*([^\s"'/>]+)""", # XHTML
            ]
        for pattern in patterns:
            if sys.version_info > (3,) and type(html) == bytes:
                pattern = bytes(pattern, 'utf-8')
            m = re.search(pattern, html, flags=re.I)
            charset = None if not m else m.group(1).decode("utf-8")
            if charset is not None and known_encoding(charset):
                charsets.add(charset.lower())

        if len(charsets) == 0:
            # no charset declared
            return guess_charset(html), 'guessed'
        if len(charsets) == 1:
            return charsets.pop(), 'declared'
        if len(charsets) > 1:
            # resolve conflict in charset declaration
            return guess_charset(html, charsets), 'resolved'

class PDFTextExtractor(BaseProcessAlgorithm):
    """ Get plain text from the PDF. """
    def _process(self, data):
        return data
