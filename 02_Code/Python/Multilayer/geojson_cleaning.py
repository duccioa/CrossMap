import re

folder_path = "/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/MultilayerParis/"
file_name = "multigraph.geojson"
file_path = folder_path + file_name

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

num_lines = file_len(file_path)
print(f"Number of lines: {num_lines}")

j = open(folder_path + file_name, "r")
txt = """
    {\n
	"type": "FeatureCollection",\n
	"features":[    
    """
types = []
for i, line in enumerate(j):
    l = len(line)
    r = re.findall('"type":"(\w+)', line)
    for type in r:
        types.append(type)
    if "\"type\":\"Feature\"" not in line:
        line = line[:l-2] + ",\"type\":\"Feature\"" + line[l-2:]
    line = re.sub("\$", "", line)
    line = line.rstrip('\n')
    if i < num_lines-1:
        line += ",\n"
    txt += line

j.close()

txt += "\n]}"
print(txt[:1000], "\n\n\n", txt[len(txt)-1000:])

with open(folder_path + "multilayer_edited.geojson", "w+") as output:
    output.write(txt)
