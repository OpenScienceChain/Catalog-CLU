#!/usr/bin/env python3

import argparse
import glob
import hashlib
import json
import os
import sys
from argparse import RawTextHelpFormatter
from urllib.parse import urlparse

import requests
import yaml
from osc.osc_utils import print_summary, save_query_result, update_summary

#####################################################
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
#####################################################


URL = "https://portal.opensciencechain.sdsc.edu/"
# URL = "https://osc-dev.ucsd.edu/"
DATA = "api/data/"
SEARCH = "api/search/"


# use this regular expression? /^10.\d{4,9}/[-._;()/:A-Z0-9]+$/i
def validate_doi(doi):
    res = 0

    # ToDo:  we are deferring this validation to the portal rest APIs
    return res


def validate_url(url):
    res = 0
    parse_res = urlparse(url)
    #  print (parse_res)

    # basic url validations. the portal rest APIs validate the urls as well.
    if parse_res.scheme == "" or parse_res.netloc == "":
        print("Please enter a valid URL")
        return -1
    if parse_res.scheme != "http" and parse_res.scheme != "https":
        print("Please enter a valid URL")
        return -1

    return res


def validate_fields(action, data):

    res = 0

    # check if at least on file is present
    if data["manifest"] == []:
        print("At least one file should be present for the '", action, "' action")
        return -1

    # check if the title is present
    if data["title"] == "":
        print("Title cannot be empty.")
        return -1

    # check if at least one of DOI or URL is present.
    if (data["url"] == "") and (data["doi"] == ""):
        print("At least one of DOI or URL must be present.")
        return -1

    # validate DOI format
    #  if (data['doi'] != ""):
    #    res = validate_doi(data['doi'])
    # validate URL format
    if data["url"] != "":
        res = validate_url(data["url"])

    return res


def map_headers():
    hlist1 = {
        "Title": "title",
        "Description": "description",
        "URL": "url",
        "DOI": "doi",
        "Acknowledgment": "acknowledgment",
    }

    hlist2 = {"Keywords": "keywords"}

    return hlist1, hlist2


# returns a list of all files to be hashed
# returns NULL if no files are directories are present
def get_all_files(data):
    all_files = set()

    if data["Files"] and (data["Files"] != ""):
        f1 = data["Files"]
        all_files = {f for f in f1}
    dir = data["Directories"]
    if data["Directories"] and (data["Directories"] != ""):
        for d in dir:
            all_files.update([f for f in glob.glob(d + "/**", recursive=True)])

    # remove the files from the exclude list
    # final_file_list = []
    ex_file_list = []
    if data["ExcludeList"] and (data["ExcludeList"] != ""):
        xl = data["ExcludeList"]
        for f in xl:
            if os.path.isfile(f):
                ex_file_list.append(f)
            elif os.path.isdir(f):
                ex_file_list.extend([fl for fl in glob.glob(f + "/**", recursive=True)])

    # assuming exclude list is a lot smaller than the actual file list. If not we need
    # to modify the way we search for an element to improve performance
    for f in ex_file_list:
        if f in all_files:
            all_files.remove(f)

    #  print ("***********************************")
    #  print(ex_file_list_dict)
    #  print ("***********************************")

    #  print(final_file_list)
    #  print ("***********************************")

    return all_files


def get_json_data(f, cli_tok, action):
    data = yaml.load(f, Loader=yaml.FullLoader)

    tok = cli_tok
    if tok == "" or tok is None:
        tok = data["Token"]
    if tok == "" or tok is None:
        print("Please submit a valid token")
        sys.exit(-1)

    files = get_all_files(data)
    json_data = {}
    hlist1, hlist2 = map_headers()

    for k, v in hlist1.items():
        if data[k] is not None:
            json_data[hlist1[k]] = data[k]
        else:
            json_data[hlist1[k]] = ""

    for k, v in hlist2.items():
        if data[k] is not None:
            json_data[hlist2[k]] = [x.strip() for x in (data[k]).split(",")]
        else:
            json_data[hlist2[k]] = []

    if action == "update":
        try:
            json_data["id"] = data["OSC-ID"]
        except Exception:
            print("OSC-ID cannot be empty\n")
            sys.exit(-1)
        try:
            if data["AssociatedID"] is not None:
                json_data["otherAssociatedIdName"] = data["AssociatedID"]
            else:
                json_data["otherAssociatedIdName"] = ""
            print(data["AssociatedID"])
        except Exception:
            json_data["otherAssociatedIdName"] = ""
        try:
            if data["AssociatedIDVal"] is not None:
                json_data["otherAssociatedIdValue"] = data["AssociatedIDVal"]
            else:
                json_data["otherAssociatedIdValue"] = ""
        except Exception:
            json_data["otherAssociatedIdValue"] = ""

        # for update, get the list of contributed files
        orig_file_list = {}
        if data["Manifest"] is None:
            print("Original contribution not found")
            sys.exit(-1)
        for i in data["Manifest"]:
            for k, v in i.items():
                orig_file_list[k] = v

    # convert funding support into appropriate format
    funding_list = []
    for i in data["Funding"]:
        #   print("funding ------ " + i)
        for k, v in i.items():
            if v:
                funding_list.append(k)

    json_data["fundingSupport"] = funding_list
    manifest_list = []

    # need for update operation
    if action == "update":
        new_files = set()
        updated_files = set()
        old_files = set()
        deleted_files = set()

    for f in files:
        sha256_hash = hashlib.sha256()
        if os.path.isfile(f):
            d = {"filename": f}
        elif os.path.isdir(f):
            continue
        else:
            print(os.getcwd())
            print("'", f, "' is not a valid file.")
            continue

        with open(f, "rb") as fin:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: fin.read(4096), b""):
                sha256_hash.update(byte_block)
            d["hash"] = sha256_hash.hexdigest()
            d["algorithm"] = "sha256"
            manifest_list.append(d)
            fin.close()

        if action == "update":
            if orig_file_list.get(f) is None:
                new_files.add(f)
            elif orig_file_list[f] == d["hash"]:
                old_files.add(f)
            elif orig_file_list[f] != d["hash"]:
                updated_files.add(f)

            deleted_files = orig_file_list.keys() - (
                new_files | old_files | updated_files
            )

    #       print(sha256_hash.hexdigest())

    json_data["manifest"] = manifest_list

    if action == "update":
        update_summary(
            new_files, old_files, updated_files, deleted_files, json_data["id"]
        )

    return json_data, tok


# contribute data
def contribute_data(f, cli_tok, osc_url, json_des_path=""):

    json_data, tok = get_json_data(f, cli_tok, "contribute")
    res = validate_fields("contribute", json_data)
    if res == -1:
        print("Please correct the errors and resubmit")
        return -1
    out = json.dumps(json_data)

    url = osc_url + DATA
    h = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "authorization": "Bearer " + tok,
    }
    res = requests.post(url, data=out, headers=h, verify=False)

    if res.status_code != requests.codes.ok:
        print("Error: " + res.text)
        return -1
    res_json = res.json()
    if res_json["docType"] == "org.osc.Error":
        print("Error: " + res_json["error_message"])
        return -1
    elif res_json["docType"] == "org.osc.AuthenticationFailed":
        print("Error: Authentication Failed. Please check your token.")
        return -1
    else:
        print("Your osc-id is " + res_json["id"])
        print("A copy of your contribution is stored as " + res_json["id"] + ".json")
        dest_path = f"{json_des_path}{res_json['id']}.json"
        with open(dest_path, "w") as fout:
            json.dump(res_json, fout)
        return res_json["id"]


# at present token is not used for search
def search_data(id, tok, osc_url):
    url = osc_url + SEARCH
    obj = {"search": id}
    out = json.dumps(obj)
    h = {"accept": "application/json", "Content-Type": "application/json"}
    #  res = requests.post(url, data=out, headers=h, verify=False)
    res = requests.post(url, data=out, headers=h, verify=True)
    if res.status_code != requests.codes.ok:
        print("Error: " + res.text)
        return -1
    res_json = res.json()
    if len(res_json) == 0:
        print("Empty")
    else:
        print("Number of matched entries: ", len(res_json))
        inp = input("Do you want to browse through all the entries (Y/N): ")
        if inp.lower() == "n":
            print("Saving the query results in yaml format, one file per matched entry")
            for i in range(0, len(res_json)):
                query_data(
                    res_json[i]["id"], ""
                )  # Search doesn't get all the fields, so need to query again based on osc-id.
            sys.exit(-1)

        for i in range(0, len(res_json)):
            print_summary(res_json[i])
            inp = input("Do you want to save this entry (Y/N): ")
            if inp.lower() == "y":
                query_data(res_json[i]["id"], "")

            if i == len(res_json) - 1:
                break
            inp = input("Do you want to browse the next entry (Y/N): ")
            if inp.lower() == "n":
                print("Ending the query...")
                sys.exit(-1)


# at present token is not used for query
def query_data(id, tok, osc_url):
    url = osc_url + DATA + id
    h = {"accept": "application/json", "Content-Type": "application/json"}
    res = requests.get(url, headers=h, verify=False)
    # res = requests.get(url, headers=h, verify=True)
    if res.status_code != requests.codes.ok:
        print("Error: " + res.text)
        return -1
    res_json_tmp = res.json()
    res_json = json.loads(res_json_tmp[0])
    if res_json["docType"] == "org.osc.Error":
        print("Error: " + res_json["info"])
    else:
        print("A copy of data from your query is stored as " + res_json["id"] + ".yaml")
        print(
            "This file should be used to update / modify the contributed dataset if you were the contributor."
        )
        save_query_result(res_json)


# update data. We are assuming that there is a json file with the contributed
# data. This function will first load the json file, convert into an yaml
# template.
# ToDo: Next user will have to update this template and "resubmit it"
def update_data(f, cli_tok, osc_url, json_result_prefix_path=""):
    json_data, tok = get_json_data(f, cli_tok, "update")
    ######################################################
    res = validate_fields("update", json_data)
    ######################################################
    if res == -1:
        print("Please correct the errors and resubmit")
        return -1
    out = json.dumps(json_data)

    ################
    url = osc_url + DATA
    #  h = {'accept': 'application/json', 'Content-Type': 'application/json', 'authorization':'Bearer ' + data['Token']}
    h = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "authorization": "Bearer " + tok,
    }
    res = requests.put(url, data=out, headers=h, verify=False)
    #  res = requests.put(url, data=out, headers=h, verify=True)

    if res.status_code != requests.codes.ok:
        print("Error: " + res.text)
        return -1
    res_json = res.json()
    #  print (res_json)
    if res_json["docType"] == "org.osc.Error":
        print("Error: " + res_json["error_message"])
    elif res_json["docType"] == "org.osc.AuthenticationFailed":
        print("Error: Authentication Failed. Please check your token.")
    else:
        print("Your osc-id is " + res_json["id"])
        print("A copy of your contribution is stored as " + res_json["id"] + ".json")
        json_path = f"{json_result_prefix_path}{res_json['id']}.json"
        with open(json_path, "w") as fout:
            json.dump(res_json, fout)
    ################


def main():
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        "operation",
        help="""'contribute', 'query' or 'update'.\n
        For update, first perform a query and then modify the saved yaml file.""",
    )
    parser.add_argument(
        "--template",
        help="template file is mandatory for the contribute and update \noperations",
    )
    parser.add_argument(
        "--oscid", help="osc-id or email-id is required for the query operation"
    )
    parser.add_argument(
        "--email", help="osc-id or email-id is required for the query operation"
    )
    parser.add_argument(
        "--token",
        help="""pass the authorization key obtained from the OSC Portal.\n
        Token is required for contribute and update operations""",
    )
    parser.add_argument(
        "--env",
        help="use the value 'dev' for the development environment. Default is the production environment.",
    )
    parser.add_argument("--version", action="version", version="%(prog)s 1.0.0")

    args = parser.parse_args()
    url = "https://portal.opensciencechain.sdsc.edu/"
    if args.env == "dev":
        url = "https://osc-dev.ucsd.edu/"

    if args.operation == "contribute":
        if args.template is None:
            print(
                "'{}' operation requires a template file in yaml format with dataset information".format(
                    args.operation
                )
            )
            sys.exit(-1)
        try:
            f = open(args.template)
        except FileNotFoundError:
            msg = "Error: File '{}' not found.".format(args.template)
            print(msg)
            sys.exit(-1)
        contribute_data(f, args.token, url)
    elif args.operation == "update":
        if args.template is None:
            print(
                "'{}' operation requires a template file in yaml format with dataset information".format(
                    args.operation
                )
            )
            sys.exit(-1)

        try:
            f = open(args.template)
        except FileNotFoundError:
            msg = "Error: File '{}' not found.".format(args.template)
            print(msg)
            sys.exit(-1)
        update_data(f, args.token, url)
    elif args.operation == "query":
        if (args.oscid is None) and (args.email is None):
            print(
                "'{}' operation requires a valid oscid or email-id".format(
                    args.operation
                )
            )
            sys.exit(-1)
        if (args.oscid is None) and (args.email is None):
            print(
                """'{}' operation does not support using both oscid and email-id simultaneously.
                Please use only one of these parameters""".format(
                    args.operation
                )
            )
        elif args.oscid is not None:
            oscid = args.oscid
            print(oscid)
            query_data(oscid, args.token, url)
        elif args.email is not None:
            search_data(args.email, args.token, url)
    else:
        msg = "'{}' operation is not supported".format(args.operation)
        print(msg)


if __name__ == "__main__":
    main()
