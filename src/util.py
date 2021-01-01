import logging
import sys
import os

def setup_logger(logger_name, file_path):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.FileHandler(file_path, encoding="utf-8")
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))