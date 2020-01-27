from pypureclient import pure1
import json

# app id is from Pure1 REST settings. (need to be set as admin to use)
app_id = "pure1:apikey:0ichbc6p0crIhWQm"
key_file = "mattapp_priv.pem"
# leave password blank if none
private_key_password = ""


# create client and authenticate
pure1Client = pure1.Client(private_key_file=key_file,
                           private_key_password=private_key_password,
                           app_id=app_id)

# get Arrays
pure1Client.get_arrays()
response = pure1Client.get_arrays()

resources = []
# check to see if the response was valid and then pull out items
if response is not None:
    resources = list(response.items)

# load previous saved arrays from file:
try:
    existing_arrays = {}
    with open("pure1_arrays.json") as f:
        existing_arrays = json.load(f)
except Exception:
    pass

# go through list of arrays from Pure1
# because
for a in resources:
    if a.id not in existing_arrays:
        existing_arrays[a.id] = a.name
        print("New array found {}".format(a.name))
    # check to see if array was renamed
    elif existing_arrays[a.id] != a.name:
        print("Array: {} was renamed to -> {} but is not new.".format(
                    existing_arrays[a.id],
                    a.name))

# save new set of arrays to file
with open("pure1_arrays.json", "w") as f:
    json.dump(existing_arrays, f)
