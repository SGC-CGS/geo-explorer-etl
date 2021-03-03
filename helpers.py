# helper functions
import datetime as dt
import gc  # for garbage collection
import json
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import zipfile as zf

# set up logger if available
log = logging.getLogger("etl_log")
log.addHandler(logging.NullHandler())


def convert_ref_year_to_date(ref_per):
    # if only year is given, set to Jan 1 for db

    if len(str(ref_per)) == 4:
        ref_per = dt.date(ref_per, 1, 1)
    retval = ref_per.strftime("%Y-%m-%d")

    return retval


def create_id_series(df, start_id):
    # returns a series of ids for a dataframe starting from start_id
    return pd.RangeIndex(start=start_id, stop=(start_id + df.shape[0]))


def daterange(date1, date2):
    # return range of dates between date1 and date2
    retval = []
    for n in range(int((date2 - date1).days) + 1):
        retval.append(date1 + dt.timedelta(n))

    return retval


# noinspection PyUnusedLocal
def delete_var_and_release_mem(var_names):
    # delete each variable in list and recover memory
    for var_name in var_names:
        del var_name
    gc.collect()


def fix_ref_year(year_str):
    # handle abnormal year formats in reference periods
    year_str = str(year_str)
    ln = len(year_str)

    if ln == 4:  # 2017 stays the same
        retval = year_str
    elif ln == 7:  # 2017/18 becomes 2018
        retval = year_str[:2] + year_str[-2:]
    elif ln == 9:  # 2017/2018 becomes 2018
        retval = year_str[-4:]
    else:
        log.warning("Invalid Reference Year: " + year_str)
        retval = 1900  # default - to help finding afterward

    return retval


def get_product_defaults(pid, pd_path):
    # read json file and return any defaults to be set on product (pid) for indicator metadata
    # examples: default breaks, colours
    prod_dict = load_json_file(pd_path)
    if pid in prod_dict:
        prod_defaults = prod_dict[pid]
    else:
        prod_defaults = prod_dict["default"]
    return prod_defaults


def get_subject_desc_from_code_set(subject_code, subject_codeset, lang):
    # retrieve unit of measure description for the specified subject_code and language (lang)
    retval = ""
    field_name = "subjectEn"
    if lang == "fr":
        field_name = "subjectFr"

    subject_code = str(subject_code)
    if subject_code != "":
        retval = next(
            (row[field_name]
             for row in subject_codeset
             if row["subjectCode"] == subject_code), None)
    return retval


def get_years_range(dt_range):
    # for dates in list (dt_range), return a distinct list of years
    retval = []
    for y in dt_range:
        yr = y.year
        if y.year not in retval:
            retval.append(yr)
    return retval


def get_uom_desc_from_code_set(uom_code, uom_codeset, lang):
    # retrieve unit of measure description for the specified uom_code and language (lang)
    retval = ""
    field_name = "memberUomEn"
    if lang == "fr":
        field_name = "memberUomFr"

    if uom_code != 0:
        retval = next(
            (row[field_name]
             for row in uom_codeset
             if row["memberUomCode"] == uom_code), None)
    return retval


def load_json_file(pd_path):
    # read json file and return dictionary
    try:
        with open(pd_path) as json_file:
            json_data = json.load(json_file)
    except IOError:
        json_data = {}
    return json_data


def setup_logger(work_dir, log_name):
    logger = logging.getLogger(log_name)
    logging.getLogger("etl_log")
    logging.basicConfig(format="%(message)s", level=logging.INFO)
    fh = logging.handlers.RotatingFileHandler(work_dir + "\\" + log_name + ".log", maxBytes=2000000, backupCount=5)
    log_fmt = logging.Formatter("%(levelname)s:%(message)s - %(asctime)s")
    fh.setFormatter(log_fmt)
    logger.addHandler(fh)  # for writing to file
    return logger


def update_merge_products_json(indicator_theme_id, merge_prod_ids, mp_path):
    # open the merge products json file (json_file) and update the dictionary of merged product ids (merge_prod_ids)
    merge_dict = load_json_file(mp_path)
    merge_dict[str(indicator_theme_id)] = {"linked_tables": [str(i) for i in merge_prod_ids]}  # all to strings for json
    retval = write_json_file(merge_dict, mp_path)
    return retval


def valid_zip_file(source_file):
    log.info("Checking " + source_file)
    retval = True
    if not zf.is_zipfile(source_file):
        log.warning("\nERROR: Not a valid zip file: " + source_file)
        retval = False
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
