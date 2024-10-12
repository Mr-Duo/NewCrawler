import pandas as pd
from imblearn.over_sampling import RandomOverSampler, SMOTE
from imblearn.under_sampling import RandomUnderSampler, OneSidedSelection
from utils.utils import *


def sample(input_dataframe: pd.DataFrame, sample_strat: ["ros", "rus", "smote", "oss"], random_state:int = 42) -> pd.DataFrame:
    X = df.drop(columns=["label"])
    y = df["label"]

    if sample_strat == "ros":
        sampler = RandomOverSampler(random_state=random_state)
    elif sample_strat == "rus":
        sampler = RandomUnderSampler(random_state=random_state)
    elif sample_strat == "smote":
        sampler = SMOTE(random_state=random_state)
    elif sample_strat == "oss":
        sampler = OneSidedSelection(random_state=random_state)

    X_sampled, y_sampled = sampler.fit_resample(X, y)
    df_sampled = pd.concat([X_sampled, y_sampled], axis=1)
    return df_sampled

file_paths = [
    "output/dataset/FFmpeg/SETUP5/unsampling/SETUP5-FFmpeg-features-train.jsonl",
    "output/dataset/FFmpeg/SETUP5/unsampling/SETUP5-FFmpeg-simcom-train.jsonl",
    "output/dataset/FFmpeg/SETUP5/unsampling/SETUP5-FFmpeg-deepjit-train.jsonl",
]

out_paths = [
    "output/dataset/FFmpeg/SETUP5/rus/SETUP5-FFmpeg-features-train.jsonl",
    "output/dataset/FFmpeg/SETUP5/rus/SETUP5-FFmpeg-simcom-train.jsonl",
    "output/dataset/FFmpeg/SETUP5/rus/SETUP5-FFmpeg-deepjit-train.jsonl",
]

path = "output/dataset/FFmpeg/SETUP5/rus"
if not os.path.exists(path):
    os.mkdir(path)
for input, output in zip(file_paths, out_paths):
    data = list()
    for line in load_jsonl(input):
        data.append(line)

    df = pd.DataFrame(data)
    df_sampled = sample(df, "rus")
    data = df_sampled.to_dict(orient = "records")
    save_jsonl(data, output)
    
    print(f"{input} to {output}")