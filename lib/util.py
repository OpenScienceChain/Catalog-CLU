import yaml
import json

# FUNDING_AGENCIES = set(["NASA", "NIH", "NOAA", "NSF"])


def create_template_yaml(file, data, hash_path, dest, token, update=False):
    with open(file, "r") as stream:
        try:
            yaml_file = yaml.safe_load(stream)
            yaml_file["Description"] = data.get("Description")
            yaml_file["Title"] = f"{data.get('Resource Name')} - SDF"
            yaml_file["URL"] = data.get("Resource URL")
            yaml_file["Keywords"] = data.get("Keywords")
            yaml_file["Acknowledgment"] = data.get("Defining Citation")
            if not update:
                yaml_file["Files"] = [hash_path]
            yaml_file["Token"] = token
            for funding in data["funding"]:
                agency = funding["agency"]
                funding_id = funding["funding_id"]

                for funding_agency in yaml_file["Funding"]:

                    if agency in funding_agency:
                        funding_agency[agency] = True
                        if funding_id:
                            funding_info = f"\n{agency}: {funding_id}"
                            if yaml_file["Acknowledgment"] is None:
                                yaml_file["Acknowledgment"] = funding_info
                            else:
                                yaml_file["Acknowledgment"] += funding_info
            with open(f"{dest}", "w") as output:
                yaml.dump(yaml_file, output)
        except yaml.YAMLError as err:
            print(err)


def get_config(config_file):
    with open(config_file, "r") as config_file:
        config = json.load(config_file)
    return config
