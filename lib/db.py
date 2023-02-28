import pymysql
import hashlib
from sshtunnel import SSHTunnelForwarder

resource_info_query = """SELECT DISTINCT
    resources.rid,
    resource_columns.name,
    resource_columns. `value`,
    resource_columns. `version`
FROM
    resource_columns
    LEFT JOIN resources ON resource_columns.rid = resources.id
    INNER JOIN (
        SELECT
            resources.rid,
            MAX(resource_columns. `version`) `version`
        FROM
            resource_columns
            LEFT JOIN resources ON resource_columns.rid = resources.id
        GROUP BY
            resources.rid) max_table ON resources.rid = max_table.rid
    AND resource_columns. `version` = max_table.version
WHERE
    resources.rid in(%s)"""

funding_info_query = """SELECT
    resource_relationships.id1,
    resource_relationships.id2
FROM
    resource_relationships
WHERE
    resource_relationships.reltype_id = 14
    AND resource_relationships.id2 in (%s)"""


osc_id_query = """SELECT
    rid,
    osc_id
FROM
    resource_to_osc_mapping"""


insert_osc_id_query = """
    INSERT INTO resource_to_osc_mapping  (rid, osc_id)
    VALUES (%s, %s)
"""


def format_data(data):
    new_data = []
    seen = set()
    new_dict = {}
    for d in data:
        rid = d["rid"]
        if rid not in seen:
            if len(new_dict) > 0:
                new_data.append(new_dict)
            seen.add(rid)
            new_dict = {"version": d["version"], "rid": rid}
        new_dict[d["name"]] = d["value"]
    new_data.append(new_dict)
    return new_data


def format_funding(resource, finding_info):
    pass


def hash_resource(resource):
    string_to_hash = "".join([str(x) for x in resource.values()])
    return hashlib.md5(string_to_hash.encode()).hexdigest()


def get_connection(user, password, db, host, port):
    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=db,
        cursorclass=pymysql.cursors.DictCursor,
        port=port,
    )


def get_connection_tunnel(tunnel, config):

    db_username = config["db"]["username"]
    db_password = config["db"]["password"]
    db_host = config["db"]["host"]
    db_name = config["db"]["name"]
    db_port = config["db"]["port"]
    if tunnel:
        ssh_username = config["ssh"]["username"]
        ssh_host = config["ssh"]["host"]
        ssh_pkey = config["ssh"]["pkey"]
        ssh_bind_address = config["ssh"]["bind_address"]
        ssh_port = config["ssh"]["port"]
        forward_tunnel = SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_username,
            ssh_pkey=ssh_pkey,
            remote_bind_address=(ssh_bind_address, db_port),
        )
        forward_tunnel.start()
        port = forward_tunnel.local_bind_address[1]
        try:
            connection = get_connection(
                db_username, db_password, db_name, db_host, port
            )
            return connection, forward_tunnel
        except Exception as err:
            print(err)
            print("Failed to Connect")
    else:
        return (
            get_connection(db_username, db_password, db_name, db_host, db_port),
            None,
        )


def get_resource_ids(conn):
    cursor = conn.cursor()
    sql = "SELECT `rid` FROM `resources` WHERE `cid`= 56 AND `status` = 'Curated'"
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result


def get_funding_info(conn, resources):
    ids = tuple([resource["rid"] for resource in resources])
    ids_format = ",".join(["%s"] * len(ids))
    cursor = conn.cursor()
    cursor.execute(funding_info_query % ids_format, (ids))
    result = cursor.fetchall()
    cursor.close()

    formatted_data = {}
    for d in result:
        if d["id2"] not in formatted_data:
            formatted_data[d["id2"]] = []
        funding = d["id1"].split("|||")
        funding_agency = funding[0]
        funding_id = funding[1]
        if funding_agency != "":
            formatted_data[d["id2"]].append(
                {
                    "agency": funding[0],
                    "funding_id": funding[1] if funding_id != "" else None,
                }
            )

    return formatted_data


def get_resource_info(conn, resources):
    ids = tuple([resource["rid"] for resource in resources])
    ids_format = ",".join(["%s"] * len(ids))
    cursor = conn.cursor()
    cursor.execute(resource_info_query % ids_format, (ids))
    result = cursor.fetchall()

    cursor.close()
    formatted_data = format_data(result)
    for d in formatted_data:
        d["hash"] = hash_resource(d)
    return formatted_data


def get_osc_ids(conn):
    cursor = conn.cursor()
    cursor.execute(osc_id_query)
    ids = cursor.fetchall()
    cursor.close()
    osc_id_dict = {}
    for osc_id in ids:
        if osc_id["rid"] not in osc_id_dict:
            osc_id_dict[osc_id["rid"]] = osc_id["osc_id"]

    return osc_id_dict


def insert_osc_id(conn, rid, osc_id):
    cursor = conn.cursor()
    cursor.execute(insert_osc_id_query, (rid, osc_id))
    conn.commit()
    cursor.close()
