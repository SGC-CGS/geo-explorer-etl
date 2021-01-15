# helper functions

import datetime as dt
import json
import pandas as pd
import zipfile as zf


def convert_ref_year_to_date(ref_per):
    # if only year is given, set to Jan 1 for db

    if len(str(ref_per)) == 4:
        ref_per = dt.date(ref_per, 1, 1)
    retval = ref_per.strftime("%Y-%m-%d")

    return retval


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

    if ln == 4:
        retval = year_str
    elif ln == 7:  # 2017/18
        retval = year_str[:2] + year_str[-2:]
    elif ln == 9:  # ex. 2017/2018
        retval = year_str[-4:]
    else:
        print("Invalid Reference Year: " + year_str)
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


def mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2  # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)


def unzip_file(source_file, target_path):
    print("Extracting " + source_file + "...\n")
    retval = False

    if zf.is_zipfile(source_file):
        try:
            with zf.ZipFile(source_file, "r") as zip_obj:
                zip_obj.printdir()
                zip_obj.extractall(target_path)
                print("\nFile extracted.")
                retval = True
        except zf.BadZipFile:
            print("\nERROR: Zip file is corrupted.")
        except FileNotFoundError:
            print("\nERROR: Zip file does not exist.")
    else:
        print("\nERROR: Not a valid zip file: " + source_file)

    return retval
