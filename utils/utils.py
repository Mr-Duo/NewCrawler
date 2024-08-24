from typing import List, Dict
import logging as log
import json
import tempfile
import heapq
import os
import uuid

def create_log_handler(name: str) -> log.Logger:
    # Construct the log filename based on the core ID
    log_filename = f'log/{name}'
    
    # Create a logger with the core-specific name
    logger = log.getLogger(name=name)
    logger.setLevel(log.INFO)  # Set the logging level to INFO
    
    # Create a file handler for logging
    fileHandler = log.FileHandler(log_filename)
    fileHandler.setLevel(log.INFO)  # Set the handler level to INFO

    # Define the log message format
    formatter = log.Formatter('%(asctime)s :: %(funcName)s - %(levelname)s :: %(message)s')
    fileHandler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(fileHandler)

    # Set specific logging levels for other modules, if needed
    log.getLogger('pydriller').setLevel(log.WARNING)

    # Log the initialization message
    logger.info(f'Logging initialized for {name}')

    return logger

def save_jsonl(data: List[Dict], output_file: str):
    try:
        with open(output_file, 'w') as f:
            for d in data:
                f.write(json.dumps(d) + '\n')
    except Exception as e:
        raise e

def append_jsonl(data: List[Dict], output_file: str):
    try:
        with open(output_file, 'a') as f:
            for d in data:
                f.write(json.dumps(d) + '\n')
    except Exception as e:
        raise e

def load_jsonl(input_file: str):
    try:
        with open(input_file, "r") as f:
            for line in f:
                yield(json.loads(line))
    except Exception as e:
        raise e

def generate_id():
    return uuid.uuid1()