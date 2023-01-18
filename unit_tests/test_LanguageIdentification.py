# coding: utf-8
from unittest import mock
from assertpy import assert_that

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from LanguageIdentification import LanguageIdentifier
from metadata import *
from config import STOPLISTS

class TestLanguageIdentification():
    
    class TestLanguageIdentifier():
    
        def test_initialize(self):
            li = LanguageIdentifier()
            assert_that(li).is_instance_of(LanguageIdentifier)
            assert_that(li.stoplists).is_instance_of(dict)
            assert_that(li.stoplists).contains_key("cs", 'sk', 'en', 'de','pl', 'ru', 'fr')

        def test_non_existing_stoplist(self):
            li = LanguageIdentifier()
            assert_that(li._load_stoplist).raises(ValueError).when_called_with('/non-existing/path')
        
        def test_stoplists(self):
            li = LanguageIdentifier()
            ret = li._load_stoplist(STOPLISTS["cs"])
            assert_that(ret).is_instance_of(list)
            assert_that(ret).contains("jsou", 'byl', 'v')
        
        def test_guess_lang(self):
            li = LanguageIdentifier()
            assert_that(li.guess_lang('')).is_none()
            
            # první odstavec definice jazyka z wikipedie
            ret = li.guess_lang('Jazyk je abstraktní struktura (řád mezi vhodnými '
                'primitivy) schopná nést informaci, a tak ji uchovávat a přenášet '
                '– sdělovat. Při aplikaci musí být materializována vhodně '
                'strukturovanou hmotou či energií (zdůvodnění viz Informace). Tak '
                'je jazyk i systém sloužící jako základní prostředek lidského '
                'dorozumívání, komunikace.[1] Kromě funkce dorozumívací může '
                'plnit další funkce, např. apelovou (může sloužit k předávání '
                'příkazů), referenční (odkazuje se na časové nebo prostorové '
                'vztahy), kontaktovou, emotivní, expresivní, estetickou (poetickou) '
                'a metajazykovou.')
            assert_that(ret).is_equal_to('cs')
            
            ret = li.guess_lang('Jazyk je historicky konštruovaný systém zvukových, '
                'lexikálnych a gramatických prostriedkov, objektivizujúci prácu '
                'myslenia, komunikácie a vzájomného chápania sa jej účastníkov. '
                'Jeho skúmaním sa zaoberá špecifická oblasť kognitívnej aktivity '
                'človeka - jazykoveda. Jeho praktickou realizáciou je reč.')
            assert_that(ret).is_equal_to('sk')
            
            ret = li.guess_lang('A language is a structured system of communication. '
                'The structure of a language is its grammar and the free components '
                'are its vocabulary. Languages are the primary means of communication '
                'of humans, and can be conveyed through speech (spoken language), '
                'sign, or writing. Many languages, including the most widely-spoken '
                'ones, have writing systems that enable sounds or signs to be recorded '
                'for later reactivation. Human language is unique among the known '
                'systems of animal communication in that it is not dependent on a '
                'single mode of transmission (sight, sound, etc.), is highly variable '
                'between cultures and across time, and affords a much wider range of '
                'expression than other systems.')
            assert_that(ret).is_equal_to('en')
            
            ret = li.guess_lang('Unter Sprache versteht man im allgemeinen Sinn '
                'alle komplexen Systeme der Kommunikation. Darunter fallen '
                'insbesondere die menschlichen natürlichen Sprachen sowie auch '
                'konstruierte Sprachen, aber auch im Tierreich existieren '
                'Zeichensysteme und Systeme von kommunikativen Verhaltensweisen, '
                'die als Sprache bezeichnet werden, etwa die Tanzsprache der '
                'Bienen. In einem erweiterten Sinn werden auch Symbolsysteme, '
                'die nur zur Repräsentation und Verarbeitung von Information '
                'dienen, als Sprache bezeichnet, etwa Programmiersprachen oder '
                'formale Sprachen in Mathematik und Logik.')
            assert_that(ret).is_equal_to('de')
            
            ret = li.guess_lang('Język – ukształtowany społecznie system '
                'budowania wypowiedzi, używany w procesie komunikacji. ')
            assert_that(ret).is_equal_to('pl')
            
            ret = li.guess_lang('Язы́к — сложная знаковая система, естественно '
                'или искусственно созданная и соотносящая понятийное содержание '
                'и типовое звучание (написание). Термин «язык», понимаемый в '
                'широком смысле, может применяться к произвольным знаковым '
                'системам, хотя чаще он используется для более узких классов '
                'знаковых систем.')
            assert_that(ret).is_equal_to('ru')
            
            ret = li.guess_lang("Le langage est la capacité d'exprimer une "
                "pensée et de communiquer au moyen d'un système de signes "
                "(vocaux, gestuel, graphiques, tactiles, olfactifs, etc.) "
                "doté d'une sémantique, et le plus souvent d'une syntaxe — mais "
                "ce n'est pas systématique (la cartographie est un exemple de "
                "langage non syntaxique). Fruit d'une acquisition, la langue "
                "est une des nombreuses manifestations du langage. ")
            assert_that(ret).is_equal_to('fr')
             
        def test_guess_lang_min_sw_ratio(self):
            li = LanguageIdentifier()
            li.stoplists = {"cs": ["a", "b"]}
            text = "Foo bar a b." # sw_ratio is 2 / 4 = 0.5
            ret = li.guess_lang(text, min_sw_ratio=1, min_sw_count=0)
            assert_that(ret).is_none()
            ret = li.guess_lang(text, min_sw_ratio=0.51, min_sw_count=0)
            assert_that(ret).is_none()
            ret = li.guess_lang(text, min_sw_ratio=0.5, min_sw_count=0)
            assert_that(ret).is_equal_to('cs')
            ret = li.guess_lang(text, min_sw_ratio=0.49, min_sw_count=0)
            assert_that(ret).is_equal_to('cs')
            ret = li.guess_lang(text, min_sw_ratio=0, min_sw_count=0)
            assert_that(ret).is_equal_to('cs')
            
        def test_guess_lang_min_sw_count(self):
            li = LanguageIdentifier()
            li.stoplists = {"cs": ["a", "b"]}
            text = "Foo bar a b." # sw_count is 2
            ret = li.guess_lang(text, min_sw_ratio=0, min_sw_count=0)
            assert_that(ret).is_equal_to('cs')
            ret = li.guess_lang(text, min_sw_ratio=0, min_sw_count=1)
            assert_that(ret).is_equal_to('cs')
            ret = li.guess_lang(text, min_sw_ratio=0, min_sw_count=2)
            assert_that(ret).is_equal_to('cs')
            ret = li.guess_lang(text, min_sw_ratio=0, min_sw_count=3)
            assert_that(ret).is_none()
           
