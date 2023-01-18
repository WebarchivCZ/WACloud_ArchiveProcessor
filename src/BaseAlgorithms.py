#!/usr/bin/python
# coding: utf-8

"""..module:: archiveprocessor.BaseAlgorithms.

..moduleauthor:: Jan Lehecka <jlehecka@ntis.zcu.cz>
"""
from __future__ import print_function
import traceback
import logging

from metadata import URL, ID
from config import LOG_FILE, LOG_LVL


class BaseAlgorithm(object):
    """Base class initializing uniform logging behavior for all child classes.

    In all child classes, the initialization code should be placed in `_init()`
    abstract method.

    """

    def __init__(self, *args, **kwargs):
        """Global initialization for all inherited algorithms."""
        logging.basicConfig(
            level=logging.getLevelName(LOG_LVL.upper()),
            filename=LOG_FILE,
            format=(
                '%(asctime)s %(levelname)s\tp%(process)05d.%(module)s: '
                '%(message)s'
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
            )
        self.logger = logging.getLogger(self.__class__.__name__)
        self._init(*args, **kwargs)

    def _init(self, *args, **kwargs):
        """Abstract method to be optionally implemented in child classes."""
        pass


class BaseProcessAlgorithm(BaseAlgorithm):
    """Base processing algorithm.

    After initialization, the `process()` method is called for every record,
    for which the algorithm is defined in processing sequence. During
    processing, it can read and update any data and metadata of the record and
    finally, it must return the updated record object back to be handed over to
    the next algorithm.

    Methods to be implemented in child classes:
        _init(self, *args, **kwargs) ... optional initialization
        _process(self, record) ... the main record-processing code

    """

    def _process(self, record):
        """Abstract processing method to be implemented in child classes."""
        raise NotImplementedError

    def process(self, record):
        """Process given record."""
        try:
            record = self._process(record)
        except Exception as e:
            self.logger.error(
                f'Error while processing record with URL {record[URL]} and '
                f'id="{record[ID]}": {e}\n{traceback.format_exc()}'
            )
        return record
