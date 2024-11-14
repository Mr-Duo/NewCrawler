import json

VFC, VIC = set(), set()

def read(file):
    VFC, VIC = set(), set()
    with open(file, "r") as f:
        for line in f:
            a = json.loads(line)
            VFC.add(a["VFC"])
            VIC.update(a["VIC"])
            
    return VFC, VIC

files = ["T_linux.jsonl", "T_linux2.jsonl", "E:\\NewCrawler\\label\\semi_trusted_data\\linux\\ST_linux.jsonl",
         "E:\\NewCrawler\\label\\semi_trusted_data\\linux\\ST_linux2.jsonl",
         "E:\\NewCrawler\\label\\semi_trusted_data\\linux\\ST_linux3.jsonl"]

for file in files:
    c, d = read(file)
    VFC.update(c)
    VIC.update(d)
    
print(len(VFC))
print(len(VIC))

VFC= list(VFC)
VIC = list(VIC)

with open("vfc.json", "w") as f:
    json.dump(VFC, f)
    
with open("vic.json", "w") as f:
    json.dump(VIC, f)