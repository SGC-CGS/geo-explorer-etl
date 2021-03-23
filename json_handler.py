# json file and data handling

import json
import logging

# set up logger if available
log = logging.getLogger("etl_log")
log.addHandler(logging.NullHandler())


def get_master_prod_id(sib_prod_id, merged_prod_dict):
    # return the master product id in merged_prod_list for a specified sibling (sib_prod_id)
    sib_prod_id = str(sib_prod_id)
    retval = ""
    for master_pid, links in merged_prod_dict.items():
        for link_pid in links["linked_tables"]:
            if sib_prod_id == str(link_pid):
                retval = str(master_pid)
                break
    return retval


def get_merged_tables_from_json(mg_path):
    # read json file and return dictionary of product ids that have been merged under a single product id
    merge_dict = load_json_file(mg_path)
    return merge_dict


def get_product_defaults(pid, pd_path):
    # read json file and return any defaults to be set on product (pid) for indicator metadata
    # examples: default breaks, colours
    prod_dict = load_json_file(pd_path)
    if pid in prod_dict:
        prod_defaults = prod_dict[pid]
    else:
        prod_defaults = prod_dict["default"]
    return prod_defaults


def get_sibling_prod_ids(master_prod_id, merged_prod_dict):
    # return a list of all sibling ids from merged_prod_dict for the specified master product (master_prod_id)
    master_prod_id = str(master_prod_id)
    try:
        sibs = merged_prod_dict[master_prod_id]["linked_tables"]
        if master_prod_id in sibs:
            sibs.remove(master_prod_id)  # we don't need the masterid in the siblings list
        sibs = [int(i) for i in sibs]  # dict values need to be int for processing
    except (ValueError, KeyError):
        sibs = []
    return sibs


def is_master_in_merged_product(prod_id, merged_prod_dict):
    # look for the product id (prod_id) in the keys of merged_prod_dict, return True if found
    retval = False
    prod_str = str(prod_id)
    merged_prod_keys = merged_prod_dict.keys()
    for master_pid in merged_prod_keys:
        if prod_str == str(master_pid):
            retval = True
            break
    return retval


def is_sibling_in_merged_product(prod_id, merged_prod_dict):
    # look for the product id (prod_id) in the values of merged_prod_dict, return True if found
    retval = False
    prod_str = str(prod_id)
    for i, links in merged_prod_dict.items():
        for link_pid in links["linked_tables"]:
            if prod_str == str(link_pid):
                retval = True
                break
    return retval


def load_json_file(pd_path):
    # read json file and return dictionary
    try:
        with open(pd_path) as json_file:
            json_data = json.load(json_file)
    except (IOError, json.JSONDecodeError):
        json_data = {}
    return json_data


def update_merge_products_json(indicator_theme_id, merge_prod_ids, mp_path):
    # open the merge products json file (json_file) and update the dictionary of merged product ids (merge_prod_ids)
    merge_dict = load_json_file(mp_path)
    merge_prod_ids = merge_prod_ids.copy()  # so orig value of list does not change
    if indicator_theme_id in merge_prod_ids:
        merge_prod_ids.remove(indicator_theme_id)
    merge_dict[str(indicator_theme_id)] = {"linked_tables": [str(i) for i in merge_prod_ids]}  # all to strings for json
    retval = write_json_file(merge_dict, mp_path)
    return retval


def write_json_file(jdict, pd_path):
    # write dictionary (jdict) to json file
    retval = True
    try:
        with open(pd_path, "w") as json_file:
            json.dump(jdict, json_file, indent=4)  # formatted json file
    except IOError:
        retval = False
    return retval
