from tqdm import tqdm
import json
import traceback
import os
from features.Kamei14 import *
from utils.utils import *

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

class Extractor:
    def __init__(self, params):
        self.file_path = params.path
        self.file_name = self.file_path.split('/')[-1]
        self.logger = create_log_handler("logs_extractor_main.log")
        self.save_path = f"{DIR_PATH}/out"
    
    def run(self):
        commit_iterator = load_jsonl(self.file_path)
        features_extractor = Kamei14(self.logger)
        
        for commit in tqdm(commit_iterator, "Processing:"):
            line = [features_extractor.process(commit)]
            append_jsonl(line, f"{self.save_path}/features-{self.file_name}")

# Example usage
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(add_help= False)
    parser.add_argument("--path", type=str, help= "Path to jsonl input file", default= f"{DIR_PATH}/input")

    params = parser.parse_args()
    ext = Extractor(params)
    ext.run()
        