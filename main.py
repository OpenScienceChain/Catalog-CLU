from lib.db import (
    get_resource_ids,
    get_resource_info,
    get_funding_info,
    get_osc_ids,
    get_connection_tunnel,
    insert_osc_id,
)
from lib.util import create_template_yaml, get_config
import argparse
import os
from osc.osc_client import query_data, update_data, contribute_data


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tunnel", action="store_true", help="SHH Tunnel server")
    parser.add_argument(
        "--working_dir",
        action="store",
        help="Directory to write hash manifest files",
        required=True,
    )
    parser.add_argument(
        "--config", action="store", help="path to config file", required=True
    )
    parser.add_argument("--dev", action="store_true", help="Use dev OSC")
    return parser.parse_args()


def get_resources(connection):
    resource_ids = get_resource_ids(connection)
    resource_info = get_resource_info(connection, resource_ids)
    funding_info = get_funding_info(connection, resource_ids)
    osc_ids = get_osc_ids(connection)
    for resource in resource_info:
        if resource["rid"] in funding_info:
            resource["funding"] = funding_info[resource["rid"]]
    return (resource_info, osc_ids)


def main():
    args = get_args()
    config = get_config(args.config)
    (db_conn, ssh_tunnel) = get_connection_tunnel(args.tunnel, config)
    resources, osc_ids = get_resources(db_conn)
    working_dir_prefix = args.working_dir
    url = (
        "https://osc-dev.ucsd.edu/"
        if args.dev
        else "https://portal.opensciencechain.sdsc.edu/"
    )
    for resource in resources:

        working_dir = f"{working_dir_prefix}/{resource['rid']}"
        if not os.path.exists(working_dir):
            os.makedirs(working_dir)
        hash_filename = f"{working_dir}/manifest.txt"

        # check if OSC entry exists
        if resource["rid"] not in osc_ids:
            # Create new entry
            try:
                with open(hash_filename, "w+") as file:
                    file.write(resource["hash"])
                create_template_yaml(
                    "./config/script_template.yaml",
                    resource,
                    hash_filename,
                    f"{working_dir}/template.yaml",
                    config["osc"]["token"],
                )
                template_file = open(f"{working_dir}/template.yaml")
            except OSError:
                print(
                    f'Could not open/write manifest or template file for {resource["rid"]}'
                )
                continue
            osc_id = contribute_data(
                template_file, "", url, json_des_path=f"{working_dir}/"
            )
            template_file.close()
            insert_osc_id(db_conn, resource["rid"], osc_id)
        else:
            # Check if hash has been changed
            try:
                hash_string = open(hash_filename, "r").readline()
            except OSError:
                print(f'Could not read manifest file for {resource["rid"]}')
                continue
            if hash_string != resource["hash"]:
                # Update entry with new hash and other fields
                with open(hash_filename, "w+") as file:
                    file.write(resource["hash"])

                current_work_dir = os.getcwd()
                os.chdir(working_dir)
                osc_id = osc_ids[resource["rid"]]
                query_data(osc_id, "", url)
                # create new template file based the one received from querying OSC
                create_template_yaml(
                    f"{osc_id}.yaml",
                    resource,
                    hash_filename,
                    f"{osc_id}.yaml",
                    config["osc"]["token"],
                    update=True,
                )
                os.chdir(current_work_dir)
                # Read template file and update OSC entry
                try:
                    generated_yaml = open(f"{working_dir}/{osc_id}.yaml")
                except OSError:
                    print("Could not open yaml file from OSC")
                    continue
                update_data(
                    generated_yaml,
                    None,
                    url,
                    json_result_prefix_path=f"{working_dir}/",
                )
    if args.tunnel:
        ssh_tunnel.stop()
    print("Done")


main()
