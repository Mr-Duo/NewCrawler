from typing import List, Dict
import logging as log
import json
import uuid

def create_log_handler(core_id: str):
    # Construct the log filename based on the core ID
    log_filename = f'log/logs_core_{core_id}.log'
    
    # Create a logger with the core-specific name
    logger = log.getLogger(name=f'core_{core_id}')
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
    logger.info(f'Logging initialized for core {core_id}')

    return logger

def save_jsonl(data: List[Dict], output_file: str):
    try:
        with open(output_file, 'w') as f:
            for d in data:
                f.write(json.dumps(d) + '\n')
        log.info(f"Successfully saved to {output_file}")
    except Exception as e:
        log.error(f"Exception {e} - Trying to save to {output_file}")

def append_jsonl(data: List[Dict], output_file: str):
    try:
        with open(output_file, 'a') as f:
            for d in data:
                f.write(json.dumps(d) + '\n')
        log.info(f"Successfully appended to {output_file}")
    except Exception as e:
        log.error(f"Exception {e} - Trying to append to {output_file}")

def generate_id():
    return uuid.uuid1()