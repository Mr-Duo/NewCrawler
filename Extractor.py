import traceback
import os
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager

from features.Kamei14 import *
from features.VCCFinder import *
from utils.utils import *
from Dict import Dict

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
        self.repo_name = None if params.repo_name is None else params.repo_name
        self.continue_run = False if params.continue_run is None else params.continue_run
        self.logger = create_log_handler("logs_extractor_main.log")
        self.save_path = f"{DEFAULT_EXTRACTED_OUTPUT}/{self.repo_name}" if params.save_path is None else params.save_path 

        if not os.path.exists(self.save_path):
            os.mkdir(self.save_path)
    
    def run(self, path: str):
        self.file_path = path
        self.file_name = self.file_path.split('/')[-1]
        print(f"\tProcess: {self.file_name}")
        futures = []
        with ProcessPoolExecutor(max_workers=3) as executor:
            executor.submit(self.process_feature_Kamei14)
            executor.submit(self.process_feature_VCCFinder)
            executor.submit(self.process_commit)
        for future in as_completed(futures):
            pass

    def process_feature_Kamei14(self):
        try:
            features_extractor = Kamei14(self.logger)
            if self.continue_run:
                features_extractor.load_state(self.save_path)
                
            iterator = load_jsonl(self.file_path)
            for commit in tqdm(iterator, "Processing Kamei14 Features:"):
                line = [features_extractor.process(commit)]
                append_jsonl(line, f"{self.save_path}/features-{self.file_name}")
                if line[0]["fix"]:
                    append_jsonl(
                        [{
                            "VFC": line[0]["commit_id"],
                            "Repository": self.repo_name
                        }], 
                        f"{self.save_path}/security-{self.file_name}"
                    )
            features_extractor.save_state(self.save_path)
        except Exception as e:
            print(line)
            self.logger.error(traceback.format_exc())
            exit()
            
    def process_feature_VCCFinder(self):
        try:
            features_extractor = VCCFinder(self.logger)
            if self.continue_run:
                features_extractor.load_state(self.save_path)
                
            iterator = load_jsonl(self.file_path)
            for commit in tqdm(iterator, "Processing VCCFinder Features:"):
                features_extractor.absorb(commit)
            features_extractor.save_state(self.save_path)
            features_extractor.release(f"{self.save_path}/vcc-features-{self.file_name}")
        except Exception as e:
            print(line)
            self.logger.error(traceback.format_exc())
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
            added, deleted = [], []
            for hunk in commit["diff"][file]["content"]:
                patch = []
                if "ab" in hunk:
                    continue
                patch.append("added")
                if "a" in hunk:
                    for line in hunk["a"]:
                        line = get_std_str(line)
                        deleted_codes.append(line)
                        patch.append(line)
                patch.append("removed")
                if "b" in hunk:
                    for line in hunk["b"]:
                        line = get_std_str(line)
                        added_codes.append(line)
                        patch.append(line)
                patch_codes.append(" ".join(patch))
        return id, message, added_codes, deleted_codes, patch_codes

    def process_commit(self):
        msg_dict, code_dict = Dict(lower=True), Dict(lower=True)
        if self.continue_run:
            msg_dict.load_state(self.save_path)
            code_dict.load_state(self.save_path)
            
        iterator = load_jsonl(self.file_path)
        for commit in tqdm(iterator, "Processing Commits:"):
            id, message, added_codes, deleted_codes, patch_codes = self.process_one_commit(commit)
            deepjit = {
                "commit_id": id,
                "messages": message,
                "code_change": "added "+" ".join(added_codes)+" removed "+" ".join(deleted_codes)+"\n"
            }
            simcom = {
                "commit_id": id,
                "messages": message,
                "code_change": "\n".join(patch_codes)
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
                exit()
                
        msg_dict.save_state(self.save_path)
        code_dict.save_state(self.save_path)
        
        pruned_msg_dict = msg_dict.prune(100000)
        pruned_code_dict = code_dict.prune(100000)
        save_jsonl([pruned_msg_dict.get_dict(), pruned_code_dict.get_dict()], f"{self.save_path}/dict-{self.repo_name}.jsonl")   


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
    
    def get_cfg(repo, continue_run):
        cfg = {
            "repo_name": repo,
            "continue_run": continue_run,
            "save_path": None
        }
        cfg = Namespace(**cfg)
        return cfg


    # path = "E:/ALL_DATA/raw_data/FFmpeg/extracted-all-{}-start-{}-end-{}.jsonl"
    # repo = "FFmpeg"
    # first = 4800
    # end = 24000
    # step = 4800
    
    # cfg = get_cfg(repo, False)
    # ext = Extractor(cfg)
    # file_path = path.format(repo, 0, step)
    # ext.run(file_path)
    
    # cfg = get_cfg(repo, True)
    # ext = Extractor(cfg)
    # for st in range(first, end, step):
    #     ed = st + step
    #     file_path = path.format(repo, st, ed)
    #     ext.run(file_path)
        
    
    # first = 24000
    # end = 120000
    # step = 24000

    # for st in range(first, end, step):
    #     ed = st + step
    #     file_path = path.format(repo, st, ed)
    #     ext.run(file_path)
    
    # path = "E:/ALL_DATA/raw_data/OpenSSL/extracted-all-{}-start-{}-end-{}.jsonl"
    # repo = "OpenSSL"
    # first = 7200
    # end = 36000
    # step = 7200
    
    # cfg = get_cfg(repo, False)
    # ext = Extractor(cfg)
    # file_path = path.format(repo, 0, step)
    # ext.run(file_path)
    
    # cfg = get_cfg(repo, True)
    # ext = Extractor(cfg)
    # for st in range(first, end, step):
    #     ed = st + step
    #     file_path = path.format(repo, st, ed)
    #     ext.run(file_path)
    
    def fetch_jsonl_files(root_dir):
        jsonl_files = []
        # Traverse the first level of directories
        for subdir, dirs, files in os.walk(root_dir):
            print(subdir, dirs, files)
                # Add .jsonl files in the subdirectories (leaves)
            jsonl_files += [os.path.join(subdir, file) for file in files if file.endswith('.jsonl')]
        return jsonl_files
    
    def extract_number1(filename):
        # Regular expression to extract 'number1' from filenames of the format 'a-number1-b-number2.jsonl'
        match = re.search(r'start-(\d+)-.+\.jsonl', filename)
        if match:
            return int(match.group(1))  # Convert number1 to an integer for sorting
        return None

    def sort_files_by_number1(file_list):
        # Sort the file list based on 'number1'
        return sorted(file_list, key=extract_number1)

    # Example usage
    root_directory = "../KaggleOutputPull"
    jsonl_files = fetch_jsonl_files(root_directory)
    jsonl_files = sort_files_by_number1(jsonl_files)
    print(jsonl_files[0])
    print(jsonl_files[1])
    
    repo = "linux"
    cfg = get_cfg(repo, False)
    ext = Extractor(cfg)
    print(jsonl_files[0])
    ext.run(jsonl_files[0])
    print("==========================================")
    cfg = get_cfg(repo, True)
    ext = Extractor(cfg)
    for i in range(1, len(jsonl_files)):
        print(jsonl_files[i])
        ext.run(jsonl_files[i])
        print("==========================================")