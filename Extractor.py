import traceback
import os
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager

from features.Kamei14 import *
from utils.utils import *
from Dict import Dict, DictManager

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

def split_sentence(sentence):
    sentence = sentence.replace('.', ' . ').replace('_', ' ').replace('@', ' @ ')\
        .replace('-', ' - ').replace('~', ' ~ ').replace('%', ' % ').replace('^', ' ^ ')\
        .replace('&', ' & ').replace('*', ' * ').replace('(', ' ( ').replace(')', ' ) ')\
        .replace('+', ' + ').replace('=', ' = ').replace('{', ' { ').replace('}', ' } ')\
        .replace('|', ' | ').replace('\\', ' \ ').replace('[', ' [ ').replace(']', ' ] ')\
        .replace(':', ' : ').replace(';', ' ; ').replace(',', ' , ').replace('<', ' < ')\
        .replace('>', ' > ').replace('?', ' ? ').replace('/', ' / ')
    sentence = ' '.join(sentence.split())
    return sentence

class Extractor:
    def __init__(self, params):
        self.repo_name = params.repo_name if params.repo_name else None
        self.discontinue_run = params.discontinue_run if params.discontinue_run else False
        self.logger = create_log_handler("logs_extractor_main.log")
        self.save_path = f"{DIR_PATH}/out/{self.repo_name}"

        if not os.path.exists(self.save_path):
            os.mkdir(self.save_path)
    
    def run(self, path: str):
        features_extractor = Kamei14(self.logger)
        self.file_path = path
        self.file_name = self.file_path.split('/')[-1]
        print(f"\tProcess: {self.file_name}")
        futures = []
        with ProcessPoolExecutor(max_workers=2) as executor:
            executor.submit(self.process_feature, features_extractor)
            executor.submit(self.process_commit)
        for future in as_completed(futures):
            pass

    def process_feature(self, features_extractor):
        try:
            iterator = load_jsonl(self.file_path)
            for commit in tqdm(iterator, "Processing Features:"):
                line = [features_extractor.process(commit)]
                append_jsonl(line, f"{self.save_path}/features-{self.file_name}")
                if line[0]["fix"]:
                    append_jsonl(
                        [{
                            "fix_commit_hash": line[0]["commit_id"],
                            "repo_name": self.repo_name
                        }], 
                        f"{self.save_path}/security-{self.file_name}"
                    )
            if self.discontinue_run:
                features_extractor.save_state(self.save_path)
        except Exception as e:
            print(traceback.format_exc())
            exit()

    def process_one_commit(self, commit):
        def get_std_str(string: str):
            return " ".join(split_sentence(string.strip()).split(" ")).lower()

        id = commit["commit_id"]
        message = get_std_str(commit["message"])
        added_codes = [] 
        deleted_codes = []
        patch_codes = []
        for file in commit['files']:
            added, deleted, patch = [], [], []
            for hunk in commit["diff"][file]["content"]:
                if "ab" in hunk:
                    continue
                if "a" in hunk:
                    for line in hunk["a"]:
                        line = get_std_str(line)
                        deleted_codes.append(line)
                        patch.append(line)
                if "b" in hunk:
                    for line in hunk["b"]:
                        line = get_std_str(line)
                        added_codes.append(line)
                        patch.append(line)
            patch_codes.append(" ".join(patch))
        return id, message, added_codes, deleted_codes, patch_codes

    def process_commit(self):
        msg_dict = Dict(lower=True)
        code_dict = Dict(lower=True)
        iterator = load_jsonl(self.file_path)
        for commit in tqdm(iterator, "Processing Commits:"):
            id, message, added_codes, deleted_codes, patch_codes = self.process_one_commit(commit)
            deepjit = {
                "commit_id": id,
                "messages": message,
                "code_change": patch_codes
            }
            simcom = {
                "commit_id": id,
                "messages": message,
                "code_change": "Removed: "+'\\n'.join(deleted_codes)+" Added: "+'\\n'.join(added_codes)+" "
            }
            try:
                append_jsonl([deepjit], f"{self.save_path}/deepjit-{self.file_name}")
                append_jsonl([simcom], f"{self.save_path}/simcom-{self.file_name}")

                for word in message.split():
                    msg_dict.add(word)
                for line in patch_codes:
                    for word in line.split():
                        code_dict.add(word)
            except Exception as e:
                self.logger.error(e)
                self.logger.error(traceback.format_exc())
        save_jsonl([msg_dict.get_dict(), code_dict.get_dict()], f"{self.save_path}/dict-{self.file_name}")


# Example usage
# if __name__ == "__main__":
#     import argparse
#     parser = argparse.ArgumentParser(add_help= False)
#     parser.add_argument("--path", type=str, help= "Path to jsonl input file", default= f"{DIR_PATH}/input")
#     parser.add_argument("--repo_name", type=str, help="Repo name")
#     parser.add_argument("--start", type=int, default=None, help= "First commit index")
#     parser.add_argument("--end", type=int, default=None, help="Last commit index")

#     params = parser.parse_args()
#     ext = Extractor(params)
#     ext.run()

if __name__ == "__main__":
    from argparse import Namespace
    cfg = {
        "repo_name": "linux",
        "discontinue_run": True
    }
    cfg = Namespace(**cfg)
    ext = Extractor(cfg)

    path = "/data1/duong/KaggleOutputPull/linux-{}-{}/extracted-all-linux-start-{}-end-{}.jsonl"
    step = 30000
    for st in range(0, 1530000, step):
        ed = st + step
        file_path = path.format(st, ed, st, ed)
        ext.run(file_path)
