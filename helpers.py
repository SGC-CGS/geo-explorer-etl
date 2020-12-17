# helper functions

import datetime as dt
import pandas as pd
import zipfile as zf


def build_column_and_type_dict(dimensions, langs):
    # set up the dicionary of columns and data types for pandas df
    # add columns listed in dimensions to the predefined columns below.
    # returns dictionary of cols:types for en and fr
    cols = {"en": {"REF_DATE": "string", "DGUID": "string", "UOM": "category", "VECTOR": "string",
                   "COORDINATE": "string", "STATUS": "category", "SYMBOL": "string",
                   "TERMINATED": "category", "VALUE": "float64"},
            "fr": {"PÉRIODE DE RÉFÉRENCE": "string", "COORDONNÉES": "string",
                   "UNITÉ DE MESURE": "category"}
            }
    for lang in langs:
        for dim in dimensions[lang]:
            cols[lang][dim] = "category"
    return cols


def build_dimension_ul(ref_year, indicator_name):
    # build custom unordered list (html) based on
    # provided reference year and indicator name columns
    dim_ul = "<ul><li>" + ref_year + "<li>" + indicator_name.str.replace(" _ ", "<li>") + "</li></ul>"
    return dim_ul


def build_indicator_code(coordinate, reference_date, pid_str):
    # build a custom indicator code that strips geography from the coordinate
    # and adds a reference date
    # IndicatorCode ex. 13100778.1.23.1.2017/2018-01-01
    temp_coordinate = coordinate.str.replace(r"^([^.]+\.)", "", regex=True)  # strips 1st dimension (geography)
    indicator_code = pid_str + "." + temp_coordinate + "." + reference_date + "-01-01"
    return indicator_code


def concat_dimension_cols(dimensions, df, delimiter):
    # concatenate data frame columns by dimension name
    #   dimensions = list of dimensions
    #   df = data frame containing columns with names that match dimension list
    #   delimiter = text to insert between dimension values
    # returns a column (pandas series) with all dimension values joined
    retval = ""
    first_col = True
    for dimension in dimensions:
        if first_col:
            retval = df[dimension]
            first_col = False
        else:
            retval += delimiter + df[dimension]
    return retval


def convert_csv_to_df(csv_file_name, delim, cols):
    # read specified csv in chunks
    # cols = dict of columns and data types colname:coltype
    # return as pandas dataframe
    prod_rows = []
    print("Reading file to dataframe: " + csv_file_name)
    for chunk in pd.read_csv(csv_file_name, chunksize=10000, sep=delim, usecols=list(cols.keys()), dtype=cols):
        prod_rows.append(chunk)
    csv_df = pd.concat(prod_rows)
    return csv_df


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
