import json
import math
import re, os
from typing import List, Set, Tuple
from tqdm import tqdm
from utils.utils import *

def get_value(file: str, key: str) -> Set[str]:
    out = set()
    for line in load_jsonl(file):
        out.add(line.get(key, None))    
    return out

def get_VFC_VIC(label_files: List[str]) -> (Set[str], Set[str]):
    VFC, VIC = set(), set()
    for label_file in label_files:
        print(label_file)
        for line in load_jsonl(label_file):
            VFC.add(line["VFC"])
            VIC.update(line["VIC"])
            
    return (VFC, VIC)

def get_non_VIC_and_security(files: List[str], VFC: Set[str], VIC: Set[str]) -> Set[str]:
    non_VIC, security = set(), set()
    for file in files:
        for commit in load_jsonl(file):
            if commit["commit_id"] not in (VFC | VIC):
                non_VIC.add(commit["commit_id"])
            if commit["fix"] == 1:
                security.add(commit["commit_id"])
    return non_VIC, security

def assign_date(target_files: List[str], label_file: str, label_set: Set[str]) -> None:
    with open(label_file, "w") as f:
        for target_file in target_files:
            for line in load_jsonl(target_file):
                if line["commit_id"] in label_set:
                    out = {
                        "commit_id": line["commit_id"],
                        "date": line["date"]
                    }
                    f.write(json.dumps(out) + "\n")
                    del out
                        
def get_data_sorted_by_date(path: str) -> List[Dict]:
    res = [v for v in load_jsonl(path)]
    res = sorted(res, key=lambda x: x["date"])
    save_jsonl(res, path)

def split_by_ratio(data: List, ratios: List) -> List[List]:
    out = []
    for r in ratios:
        start = math.ceil(r[0] * len(data))
        end = math.ceil(r[1] * len(data))
        out.append(data[start:end])
    return out

def split_by_date(data: List, date: List[Tuple[int, int]]) -> List[List]:
    out = [[] for i in range(len(date))]
    for d in data:
        for i, (start, end) in enumerate(date):
            if d["date"] >= start and d["date"] <= end:
                out[i].append(d)
    return out

def de_date(data: List[List[Dict]]) -> List[List[str]]:
    res = []
    for dtset in data:
        res.append([])
        for point in dtset:
            res[-1].append(point["commit_id"])
            
    return res

# def to_file(in_files: str, out_file: str, label1: List, label0: List) -> None:
#     with open(out_file, "w") as f:
#         for file in in_files:
#             for line in load_jsonl(file):
#                 if line["commit_id"] in label0:
#                     line["label"] = 0
#                     f.write(json.dumps(line) + "\n")
#                 elif line["commit_id"] in label1:
#                     line["label"] = 1
#                     f.write(json.dumps(line) + "\n")
                            
def to_dataset(project: str, out_folder: str, label1s: List[List[List]], label0s: List[List[List]]) -> None:
    out_folder = f"{out_folder}/{setup}"
    if not os.path.exists(out_folder):
        os.mkdir(out_folder)
    if not os.path.exists(f"{out_folder}/unsampling"):
        os.mkdir(f"{out_folder}/unsampling")
    datasets = ["train", "val", "test"]
    
    input_files = {
        "features": find_files(FEATURES_PATERN, f"{DEFAULT_EXTRACTED_OUTPUT}/{project}"),
        "simcom": find_files(SIMCOM_PATERN, f"{DEFAULT_EXTRACTED_OUTPUT}/{project}"),
        "deepjit": find_files(DEEPJIT_PATERN, f"{DEFAULT_EXTRACTED_OUTPUT}/{project}"),
        "vcc-features": find_files(VCCFINDER_PATERN, f"{DEFAULT_EXTRACTED_OUTPUT}/{project}")
    }
    
    output_files = { 
        part: {
                "train": {setup : f"{out_folder}/SETUP{setup+1}/unsampling/SETUP{setup+1}-{project}-{file}-train.jsonl" for setup in range(5)},
                "val": {setup : f"{out_folder}/SETUP{setup+1}/SETUP{setup+1}-{project}-{file}-val.jsonl" for setup in range(5)},
                "test": {setup : f"{out_folder}/SETUP{setup+1}/SETUP{setup+1}-{project}-{file}-test.jsonl" for setup in range(5)}
            } 
        for part in ["features", "simcom", "deepjit", "vcc-features"]
    }
    
    log.info("Dataset!")
    for part in ["features", "simcom", "deepjit", "vcc-features"]:
        for file in input_files:
            with tqdm(desc= file) as bar:
                for line in load_jsonl(file):
                    for setup in range(5):
                        for dataset, label0, label1 in zip(datasets, label0s, label1s):
                            if line["commit_id"] in label0:
                                line["label"] = 0
                                append_jsonl([line], output_files[part][dataset][setup])
                            elif line["commit_id"] in label1:
                                line["label"] = 1
                                append_jsonl([line], output_files[part][dataset][setup]) 
                    bar.update(1)                  
     
def check_before_run(output_folder: str) -> bool:
    if not os.path.exists(f"{output_folder}/UNSPLIT/VIC.jsonl"):
        return False
    if not os.path.exists(f"{output_folder}/UNSPLIT/VFC.jsonl"):
        return False
    if not os.path.exists(f"{output_folder}/UNSPLIT/non_VIC.jsonl"):
        return False
    if not os.path.exists(f"{output_folder}/UNSPLIT/security.jsonl"):
        return False
    return True

if __name__ == "__main__":
    log = create_console_log_handler("console")
    log.info("Start!!")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_folder", type=str, default="label")
    parser.add_argument("--output_folder", type=str, default=None)
    parser.add_argument("--project", type=str, required=True)
    parser.add_argument("--continue_run", action="store_true")
    params = parser.parse_args()
    
    input_folder = params.input_folder
    output_folder = params.output_folder if params.output_folder else DEFAULT_DATA_OUTPUT
    project = params.project
    output_folder = f"{output_folder}/{project}"
    
    trusted_file = TRUSTED_LABEL_PATERN.format(project)
    semi_trusted_file = SEMI_TRUSTED_LABEL_PATERN.format(project)
    
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    
    if not params.continue_run or not check_before_run(output_folder):    
        T_VFC, T_VIC = get_VFC_VIC(find_files(trusted_file, f"{input_folder}/trusted_data/{project}"))
        ST_VFC, ST_VIC = get_VFC_VIC(find_files(semi_trusted_file, f"{input_folder}/semi_trusted_data/{project}"))
        
        VFC = T_VFC | ST_VFC
        VIC = T_VIC | ST_VIC
        del T_VFC, T_VIC, ST_VFC, ST_VIC
        
        log.info("Complete get VFC, VIC")
        feature_files = find_files(FEATURES_PATERN, f"{DEFAULT_EXTRACTED_OUTPUT}/{project}")
        non_VIC, security = get_non_VIC_and_security(feature_files, VFC, VIC)
        log.info("Complete get nonVIC")
        if not os.path.exists(f"{output_folder}/UNSPLIT"):
            os.mkdir(f"{output_folder}/UNSPLIT")
        assign_date(feature_files, f"{output_folder}/UNSPLIT/VIC.jsonl", VIC)
        assign_date(feature_files, f"{output_folder}/UNSPLIT/VFC.jsonl", VFC)
        assign_date(feature_files, f"{output_folder}/UNSPLIT/non_VIC.jsonl", non_VIC)
        assign_date(feature_files, f"{output_folder}/UNSPLIT/security.jsonl", security)
        log.info("Complete assign date!")
        
        get_data_sorted_by_date(f"{output_folder}/UNSPLIT/VIC.jsonl")
        get_data_sorted_by_date(f"{output_folder}/UNSPLIT/VFC.jsonl")
        get_data_sorted_by_date(f"{output_folder}/UNSPLIT/non_VIC.jsonl")
        get_data_sorted_by_date(f"{output_folder}/UNSPLIT/security.jsonl")
        log.info("Sorted by date")
        del VIC, VFC, non_VIC
        
    VIC = read_jsonl(f"{output_folder}/UNSPLIT/VIC.jsonl")
    VFC = read_jsonl(f"{output_folder}/UNSPLIT/VFC.jsonl")
    non_VIC = read_jsonl(f"{output_folder}/UNSPLIT/non_VIC.jsonl")
    security = read_jsonl(f"{output_folder}/UNSPLIT/security.jsonl")
    
    ratios = [(0, 0.75), (0.75, 0.8), (0.8, 1)]
    VIC = split_by_ratio(VIC, ratios)
    date = [(v[0]["date"], v[-1]["date"]) for v in VIC]
    VFC = split_by_date(VFC, date)
    non_VIC = split_by_date(non_VIC, date)
    log.info("Splitted")
    VIC = de_date(VIC)
    VFC = de_date(VFC)
    non_VIC = de_date(non_VIC)
    
    if not params.continue_run or not os.path.exists(f"{output_folder}/UNSPLIT/nonSECnonVIC.jsonl"):
        non_sec_non_VIC = [[elem for elem in sublist if elem not in security] for sublist in non_VIC]
        save_jsonl(non_sec_non_VIC, f"{output_folder}/UNSPLIT/nonSECnonVIC.jsonl")
    else:
        non_sec_non_VIC = read_jsonl(f"{output_folder}/UNSPLIT/nonSECnonVIC.jsonl")
        
    if not params.continue_run or not os.path.exists(f"{output_folder}/UNSPLIT/nonSECVFC.jsonl"):
        non_sec_VFC = [[elem for elem in sublist if elem not in security] for sublist in VFC]
        save_jsonl(non_sec_VFC, f"{output_folder}/UNSPLIT/nonSECVFC.jsonl")
    else:
        non_sec_VFC = read_jsonl(f"{output_folder}/UNSPLIT/nonSECVFC.jsonl")

    
    log.info("Fetch security commit")
    label0s = [VIC, VIC, VIC, VIC, VIC]
    label1s = [VFC, non_VIC, [i + j for i, j in zip(VFC, non_VIC)], non_sec_non_VIC, [i + j for i, j in zip(non_sec_VFC, non_sec_non_VIC)]]
    
    to_dataset( project, output_folder, label0s, label1)  
    
    log.info("Complete!")