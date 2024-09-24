from typing import List, Dict
import logging as log
import json
import tempfile
import heapq
import os
import uuid

def create_log_handler(name: str) -> log.Logger:
    log_filename = f'log/{name}'    
    logger = log.getLogger(name=name)
    logger.setLevel(log.INFO)  
    fileHandler = log.FileHandler(log_filename)
    fileHandler.setLevel(log.INFO) 
    formatter = log.Formatter('%(asctime)s :: %(funcName)s - %(levelname)s :: %(message)s')
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)
    log.getLogger('pydriller').setLevel(log.WARNING)
    logger.info(f'Logging initialized for {name}')
    return logger

def save_json(data: Dict, output_file: str) -> None:
    try:
        with open(output_file, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        raise e

def load_json(file_path: str) -> Dict:
    try:
        with open(file_path, "r") as f:
            out_dict = json.load(f)
        return out_dict
    except Exception as e:
        raise e

def save_jsonl(data: List[Dict], output_file: str) -> None:
    try:
        with open(output_file, 'w') as f:
            for d in data:
                f.write(json.dumps(d) + '\n')
    except Exception as e:
        raise e

def append_jsonl(data: List[Dict], output_file: str) -> None:
    try:
        with open(output_file, 'a') as f:
            for d in data:
                f.write(json.dumps(d) + '\n')
    except Exception as e:
        raise e

def load_jsonl(file_path: str) -> Dict:
    try:
        with open(file_path, "r") as f:
            for line in f:
                yield json.loads(line)
    except Exception as e:
        raise e

def load_chunk_jsonl(file_path: str, start: int, end: int) -> Dict:
    try:
        with open(file_path, 'r') as file:
            file.seek(start)
            if start != 0:
                file.readline() 
            while file.tell() < end:
                line = file.readline().strip()
                if not line:
                    break
                yield json.loads(line)
    except Exception as e: 
        raise e

def generate_id():
    return uuid.uuid1()