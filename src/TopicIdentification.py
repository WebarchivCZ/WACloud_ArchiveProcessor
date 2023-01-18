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

class TopicIdentifier(BaseProcessAlgorithm):
    """ Module to predict topics from extracted plain text for each record.
    
    A trained model must be present in path defined by TOPICS_CLF_MODEL.
    
    """
    
    def _init(self):
        self.tokenizer = None

        file_path = TOPICS_CLF_MODEL
        if not os.path.isfile(file_path):
            raise ValueError(f'Failed to load classifier from {file_path}.')
        self.logger.info(f'Loading trained classifier from {file_path}...')
        with open(file_path, 'rb') as fr:
            self.doc_vect, self.topic_vect, self.clf = pickle.load(fr)

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

        doc = ' '.join(tokens)
        X = self.doc_vect.transform([doc])
        y_soft = self.clf.decision_function(X)
        y = y_soft > 0
        topics = self.topic_vect.inverse_transform(y)[0]
        record[TOPICS] = topics.tolist()
        self.logger.debug(f'Predicted {len(topics)} topics: {" ".join(topics)} '
            f'(URL {record[URL]} and ID="{record[ID]}")')
        return record
    
    def generate_topics_json(self, fn):
        """ Generate a file which can be used to update HBase row in config table. """
        # convert numpy ints to serializable python ints
        vocab = dict([(t, int(id)) for t, id in self.topic_vect.vocabulary_.items()])
        data = {"topics": {"value": vocab}}
        json.dump(data, open(fn, "w"))

if __name__== "__main__":
    ti = TopicIdentifier()
    ti.generate_topics_json("TopicIdentification.json")
    
    