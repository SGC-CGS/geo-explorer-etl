# helper functions

import datetime as dt
import zipfile as zf


def daterange(date1, date2):
    # return range of dates between date1 and date2
    retval = []
    for n in range(int((date2 - date1).days) + 1):
        retval.append(date1 + dt.timedelta(n))
    return retval


def convert_ref_year_to_date(ref_per):
    # if only year is given, set to Jan 1 for db
    if len(str(ref_per)) == 4:
        ref_per = dt.date(ref_per, 1, 1)
    retval = ref_per.strftime("%Y-%m-%d")
    return retval


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
