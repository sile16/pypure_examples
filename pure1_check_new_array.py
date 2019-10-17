from pypureclient import pure1
import json

app_id = "pure1:apikey:0ichbc6p0crIhWQm"
key_file = "mattapp_priv.pem"

pure1Client = pure1.Client(private_key_file=key_file, app_id=app_id)
pure1Client.get_arrays()
response = pure1Client.get_arrays()

resources = []
if response is not None:
    resources = list(response.items)

existing_arrays = {}
try:
    with open("pure1_arrays.json") as f:
        existing_arrays = json.load(f)
except:
        pass

for a in resources:
    if a.id not in existing_arrays:
        existing_arrays[a.id] = a.name
        print("New array found {}".format(a.name))

with open("pure1_arrays.json", "w") as f:
    json.dump(existing_arrays, f)




