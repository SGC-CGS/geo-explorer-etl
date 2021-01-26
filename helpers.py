# helper functions
import datetime as dt
import json
import logging
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


def get_product_defaults(pid):
    # read json file and return any defaults to be set on product (pid) for indicator metadata
    # examples: default breaks, colours
    with open("product_defaults.json") as json_file:
        prod_dict = json.load(json_file)
        if pid in prod_dict:
            prod_defaults = prod_dict[pid]
        else:
            prod_defaults = prod_dict["default"]
        return prod_defaults


def get_years_range(dt_range):
    # for dates in list (dt_range), return a distinct list of years
    retval = []
    for y in dt_range:
        yr = y.year
        if y.year not in retval:
            retval.append(yr)
    return retval


def mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2  # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)


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


def unzip_file(source_file, target_path):
    log.info("Extracting " + source_file + ": " + str(dt.datetime.now()))
    retval = False

    if zf.is_zipfile(source_file):
        try:
            with zf.ZipFile(source_file, "r") as zip_obj:
                zip_obj.printdir()
                zip_obj.extractall(target_path)
                log.info("\nFile extracted.")
                retval = True
        except zf.BadZipFile:
            log.warning("\nERROR: Zip file is corrupted.")
        except FileNotFoundError:
            log.warning("\nERROR: Zip file does not exist.")
    else:
        log.warning("\nERROR: Not a valid zip file: " + source_file)

    return retval
