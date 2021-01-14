import logging
from logging import Formatter, Handler
import servicemanager
import sys
import os

class ServiceManagerLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            if record.levelno >= logging.ERROR:
                servicemanager.LogErrorMsg(msg)
            elif record.levelno >= logging.INFO:
                servicemanager.LogInfoMsg(msg)
        except Exception:
            pass

def setup_logger(logger_name):
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    handler = ServiceManagerLogHandler()
    handler.setFormatter(formatter)
    
    logger = logging.getLogger(logger_name)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def setup_file_logger(logger_name):
    logger = logging.getLogger(logger_name)
    path = 'C:\\Users\\Scott\\TradingMonkey_ETL\\TradingMonkey_ETL.log'
    handler = logging.FileHandler(path, encoding='utf-8')
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))