import argparse, os
from Miner import Miner
from Extractor import Extractor

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

def main():
    parser = argparse.ArgumentParser(add_help= False)
    parser.add_argument("--workers", type= int, default= 1, help="Number of parallel workers")
    parser.add_argument("--language", type= str, help="Language")
    parser.add_argument("--url", type=str, help= "Git clone url")
    parser.add_argument("--path", type=str, help= "Local Repo path", default= f"{DIR_PATH}/input")
    parser.add_argument("--start", type=int, default=None, help= "First commit index")
    parser.add_argument("--end", type=int, default=None, help="Last commit index")

    params = parser.parse_args()
    miner = Miner(params)
    out_file = miner.run()

    cfg = {
        "path": out_file
    }
    cfg = argparse.Namespace(**cfg)
    extractor = Extractor(cfg)
    extractor.run()

if __name__ == "__main__":
    main()
