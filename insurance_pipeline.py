import xmltodict
import json
import pandas as pd

params = {"state": "Alabama"}

with open("insurance_data/{}.xml".format(params["state"]),"r") as file:
    xml = file.read()

xml_dict = xmltodict.parse(xml)
json_data = json.dumps(xml_dict)
json_dict = json.loads(json_data)
data = json_dict['r539cyState']['week']

data_rows = {k:[] for k in data[0].keys()}

for i in range(len(data)):
    for k in data[i].keys():
        data_rows[k].append(data[i][k])

df = pd.DataFrame({k:data_rows[k] for k in data_rows.keys()})

print(df.head())