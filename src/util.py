import logging
from logging import Formatter, Handler
import servicemanager
import sys
import os

class _Handler(Handler):
    def emit(self, record):
        servicemanager.LogInfoMsg(record.getMessage())

def setup_logger(logger_name):
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    handler = _Handler()
    handler.setFormatter(formatter)
    
    logger = logging.getLogger(logger_name)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))