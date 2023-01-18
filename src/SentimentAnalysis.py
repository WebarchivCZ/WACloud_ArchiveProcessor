# coding: utf-8
from __future__ import print_function
import numpy as np
import pickle
import json
import os

from BaseAlgorithms import BaseProcessAlgorithm
from Tokenization import WordTokenizer
from metadata import *
from config import *

class SentimentAnalyzer(BaseProcessAlgorithm):
    """ Module to predict sentiment of extracted plain text for each record.
    
    A trained model must be present in path defined by SENTIMENT_CLF_MODEL.
    
    Predicted sentiment is a float in <-1,1> (-1 is the most negative sentiment,
    1 the most positive and 0 is neutral).
    
    """
    
    def _init(self):
        self.tokenizer = None
        file_path = SENTIMENT_CLF_MODEL
        if not os.path.isfile(file_path):
            raise ValueError(f'Failed to load classifier from {file_path}.')
        self.logger.info(f'Loading trained classifier from {file_path}...')
        with open(file_path, 'rb') as fr:
            self.doc_vect, self.clf = pickle.load(fr)
        self.logger.debug(f'Classifier loaded: {self.clf}')
    
    def _process(self, record):
        if record[LANGUAGE] != 'cs':
            return record
        
        if not record[TOKENS] and record[PLAINTEXT]:
            if self.tokenizer is None:
                self.tokenizer = WordTokenizer()
            record = self.tokenizer.process(record)
        
        tokens = record[TOKENS] or []
        if not tokens:
            return record

        def np_softmax(logits):
            return np.exp(logits) / np.sum(np.exp(logits), axis=1)[:, np.newaxis]

        doc = ' '.join(tokens)
        X = self.doc_vect.transform([doc])
        y_soft = self.clf.decision_function(X)
        y_soft = np_softmax(y_soft) # prob. distribution: [p(POS), p(NEG)], sum(.)=1
        y_soft = y_soft[:,0] - y_soft[:,1] # float value from interval [-1,1]
        s = y_soft[0]
        record[SENTIMENT] = s
        self.logger.debug(f'Predicted {"positive" if s>0 else "negative"} '
            f'sentiment ({s:.2f}) for URL {record[URL]} and ID="{record[ID]}"')
        return record

if __name__== "__main__":
    sa = SentimentAnalyzer()
    for phrase in ["Výborné, jsem nadšen.", "To je hnus velebnosti."]:
        record = {LANGUAGE: "cs", PLAINTEXT: phrase, TOKENS: [], URL:"", ID:""}
        record = sa.process(record)
        print(f'Sentiment of "{phrase}" is {record[SENTIMENT]}.')
    