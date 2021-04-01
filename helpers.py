# helper functions
import datetime as dt
import gc  # for garbage collection
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import zipfile as zf

# set up logger if available
log = logging.getLogger("etl_log")
log.addHandler(logging.NullHandler())


def build_freq_code_to_pd_dict():
    # build a dictionary of pandas date formats based on WDS codes that indicate how often the data is published).
    freq_dict = {
        1: "D",  # daily
        2: "W",  # weekly (sun)
        4: "2W",  # every 2 weeks (sun)
        6: "MS",  # monthly as start of month
        7: "2MS",  # every 2 months, interpreted as every 2 months at start of month
        9: "QS",  # quarterly as start of quarter
        10: "4MS",  # 3 times per year, interpreted as every 4 months at start of month
        11: "6MS",  # semi-annual, interpreted as every 6 months at start of month
        12: "AS",  # annual as start of year
        13: "2AS",  # every 2 years, interpreted as every 2 years at start of year
        14: "3AS",  # every 3 years, interpreted as every 3 years at start of year
        15: "4AS",  # every 4 years, interpreted as every 4 years at start of year
        16: "5AS",  # every 5 years, interpreted as every 5 years at start of year
        17: "10AS",  # every 10 years, interpreted as every 10 years at start of year
        18: "AS",  # occasional (assumed as annual as start of year)
        19: "QS",  # occasional quarterly (assumed as start of quarter)
        20: "MS",  # occasional monthly (assumed as start of month)
        21: "D"  # occasional daily (assumed as daily)
    }
    return freq_dict


def combine_ordered_lists(list1, list2):
    # append unique values from list2 to end of list1. ensures list1 stays in original order.
    retval = list1
    for val in list2:
        if val not in list1:
            retval.append(val)
    return retval


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


def get_nth_item_from_string_list(item_list, delim, n=None):
    # Convert string to list with delimiter and return the nth member of the list. If none given, return last item.
    # Example: "Property with multiple residential units _ Vacant land _ Number of owners" --> "Number of owners"
    split_ind = item_list.split(delim)  # separator between dimensions/members
    n = -1 if n is None else n
    try:
        retval = split_ind[n]
    except IndexError:
        retval = ""
    return retval


def get_partitioned_string(search_str, delim):
    # Search string (search_str) for first match of specified delimiter (delim).
    # Example: "Crime and justice/Crimes and offences/Homicides" --> "Crimes and offences/Homicides"
    str_tup = search_str.partition(delim)  # tuple ex. ("Crime and justice", "/" , "Crimes and offences/Homicides")
    retval = str_tup[2] if str_tup[2] != "" else search_str  # if no match on delim, send back original str
    return retval


def get_subject_desc_from_code_set(subject_code, subject_codeset, lang):
    # retrieve unit of measure description for the specified subject_code and language (lang)
    retval = ""
    field_name = "subjectEn"
    if lang == "fr":
        field_name = "subjectFr"

    subject_code = str(subject_code)
    if subject_code != "":
        retval = next(
            (get_partitioned_string(row[field_name], "/")
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


def setup_logger(work_dir, log_name):
    logger = logging.getLogger(log_name)
    logging.getLogger("etl_log")
    logging.basicConfig(format="%(message)s", level=logging.INFO)
    fh = logging.handlers.RotatingFileHandler(work_dir + "\\" + log_name + ".log", maxBytes=2000000, backupCount=5)
    log_fmt = logging.Formatter("%(levelname)s:%(message)s - %(asctime)s")
    fh.setFormatter(log_fmt)
    logger.addHandler(fh)  # for writing to file
    return logger


def valid_zip_file(source_file):
    log.info("Checking " + source_file)
    retval = True
    if not zf.is_zipfile(source_file):
        log.warning("\nERROR: Not a valid zip file: " + source_file)
        retval = False
    return retval
