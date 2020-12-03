# Download data from WDS and update database

import config as cfg  # configuration
import datetime
import helpers as h  # helper functions
import pandas as pd
import pathlib
import scdb  # database class
import scwds  # wds class
import time
from zipfile import ZipFile

# set up
WORK_DIR = str(pathlib.Path(__file__).parent.absolute())  # current script path

# flags to turn some parts of code on/off (to ease debugging) - set all to true to run full code
download_and_unzip_product = True  # if product tables are already downloaded can set to false
create_dguid_vector_files = True  # if dguid/vector matching files have already been created can set to false
download_and_unzip_delta = True  # if delta files are already downloaded can set to false
create_delta_subset_file = True  # if delta subset files have already been built can set to false
skip_test_delta_file = False  # if you only want to use the specified test file for processing, set to false

# TEST DATE: Oct 28, 2020 has 3 tables from db with updates (46100027, 46100053, 46100054)
# delta file for Oct 28, 2020: 196 MB zipped, 2.5 GB unzipped - 32,538,310 rows.
# CSGE product changes in delta file:
#   46100027 - 4,513,250 rows
#   46100053 - 13,869,198 rows
#   46100054 - 13,869,198 rows
# Other product changes in delta file (with row counts):
#   33100036 (72), 10100139 (108), 13100784 (4536), 18100212 (16), 23100287 (3), 13100785 (2,964),
#   13100768 (7,365), 13100783 (44,828), 33100270 (223,244), 23100216 (3,528)
start_date = datetime.date(2020, 10, 27)  # y m d
end_date = datetime.date(2020, 10, 28)

if __name__ == "__main__":

    print("Script start: " + str(datetime.datetime.now()) + "\n")
    print("Looking for updates from " + str(start_date) + " to " + str(end_date) + ":")

    # create wds and db objects
    wds = scwds.serviceWds(cfg.sc_conn["wds_url"], cfg.sc_conn["delta_url"])
    db = scdb.sqlDb(cfg.sql_conn["driver"], cfg.sql_conn["server"], cfg.sql_conn["database"])

    # loop through specified date range
    for dt in h.daterange(start_date, end_date):
        rel_date = dt.strftime("%Y-%m-%d")

        # find out which cubes have changed
        print("\nLooking for changed cubes on: " + rel_date)
        changed_cubes = wds.get_changed_cube_list(rel_date)

        # check the database for matching tables
        products_to_update = db.get_matching_product_list(changed_cubes)
        if len(products_to_update) == 0:
            print("There are no tables to update for " + rel_date)  # No tables = No further action
        else:
            print("Found " + str(len(products_to_update)) + " tables to update: " + str(products_to_update))

            # Download delta file for release date
            delta_date = dt.strftime("%Y%m%d")
            delta_file_path = WORK_DIR + "\\delta_" + delta_date
            if download_and_unzip_delta:
                delta_file = wds.get_delta_file(delta_date, delta_file_path + ".zip")
                if delta_file:
                    print("Extracting " + delta_file_path + ".zip")
                    with ZipFile(delta_file_path + ".zip", "r") as zipObj:
                        zipObj.extractall(delta_file_path)

            # Download the table for each product - and create a new file w/ just the DGUID and vector
            #   TODO: Find out if there is a better way to do this - some geo info is available in
            #       the metadata, but could not find all info needed to build DGUID.
            vector_file_list = {}
            for pid in products_to_update:
                print("Downloading full table for " + str(pid))
                full_prod_file_path = WORK_DIR + "\\" + str(pid) + "-en"
                file_ext = ".zip"
                if download_and_unzip_product:
                    full_prod_file = wds.get_full_table_download(pid, "en", full_prod_file_path + file_ext)
                    if full_prod_file:
                        print("Extracting " + full_prod_file_path)
                        with ZipFile(full_prod_file_path + file_ext, "r") as zipObj:
                            zipObj.extractall(full_prod_file_path)

                # save to file with vector and DGUID only
                vector_file = WORK_DIR + "\\vector_dguid_" + str(pid) + ".csv"
                if create_dguid_vector_files:
                    dguid_rows = []
                    print("Building DGUID and vector list from full table")
                    for chunk in pd.read_csv(full_prod_file_path + "\\" + str(pid) + ".csv", chunksize=10000,
                                             usecols=["DGUID", "VECTOR"]):
                        dguid_rows.append(chunk)

                    vector_df = pd.concat(dguid_rows)  # add all DGUIDs/vectors to data frame
                    # remove 'v' character from vector id (so it can be matched to delta records)
                    vector_df["VECTOR"] = vector_df["VECTOR"].str.replace("v", "", case=False)
                    dguid_rows = None
                    print("Creating DGUID file for " + str(len(vector_df.index)) + " vectors: " + vector_file)
                    vector_df.to_csv(vector_file, encoding='utf-8', index=False)  # exclude index column
                    vector_df = None

                vector_file_list[pid] = vector_file

            # Search the delta file for each affected product and create subsetted delta files.
            #   Ran into memory issues trying to hold too much data in the list at once, so building data list for
            #   each individual product to be updated and releasing objects as soon as possible.
            delta_file_prefix = WORK_DIR + "\\filtered_delta_"
            delta_file_suffix = "_" + delta_date + ".csv"
            pid_filename_list = {}  # list of subsetted delta files

            # for counting total number of rows in delta
            first_pass = True
            row_count = 0

            print("Reading delta file...")
            for pid in products_to_update:
                pid_filename = delta_file_prefix + str(pid) + delta_file_suffix

                if create_delta_subset_file:
                    print("Searching delta file for product: " + str(pid))
                    # noinspection PyRedeclaration
                    pid_data_list = []  # rows of data for current pid

                    # read large delta file in chunks (# rows)
                    for chunk in pd.read_csv(delta_file_path + "\\" + delta_date + ".csv", chunksize=10000):
                        pid_data_list.append(chunk[chunk["productId"] == pid])  # if pid is found add to dataset
                        if first_pass:  # only count total rows on the first pid
                            row_count += len(chunk)

                    # convert list to dataframe and save to file
                    pid_df = pd.concat(pid_data_list)
                    pid_data_list = None
                    print("Creating delta subset file: " + pid_filename +
                          "\n (" + str(len(pid_df.index)) + "/" + str(row_count) + " rows)")
                    pid_df.to_csv(pid_filename, encoding='utf-8', index=False)  # exclude index column
                    pid_df = None

                pid_filename_list[pid] = pid_filename
                first_pass = False

            # TESTING --> reset to test subset file for testing purposes only (543 records) REMOVE WHEN FINISHED TODO
            if not skip_test_delta_file:
                pid_filename_list[46100027] = WORK_DIR + "\\TEST_filtered_delta_46100027_20201028.csv"
                products_to_update = [46100027]

            # read each delta file subset and look for updates
            for pid in products_to_update:
                if pid_filename_list[pid]:
                    df = pd.read_csv(pid_filename_list[pid], header=0, skip_blank_lines=True)
                    for index, row in df.iterrows():  # each row in data frame
                        # FIELD LIST:
                        #   productId, coordinate, vectorId, refPer, refPer2, symbolCode, statusCode,
                        #   securityLevelCode, value, releaseTime, scalarFactorCode, decimals, frequencyCode

                        # 3 paths for updating records (per PD) TODO
                        # Case 1. Match on vector, no match on reference period - add new reference period
                        # Case 2. Match on vector and reference period, no match on DGUID - add new geography (DGUID)
                        # Case 3. Match on vector and reference period and DGUID (plus "r" or some text? - revise data

                        # Look for matching vector and reference period
                        print("Checking vector " + str(row["vectorId"]) + " for reference period " + str(row["refPer"]))
                        if db.vector_and_ref_period_match(row["vectorId"], row["refPer"]):
                            print("Matched " + str(row["vectorId"]) + " " + str(row["refPer"]))
                            # Check DGUID - not in the delta file
                        else:
                            print("New Reference Period " + str(row["vectorId"]) + " " + str(row["refPer"]))
                            # Trigger Case 1

    # delete the objects
    db = None
    wds = None

    print("\nScript end: " + str(datetime.datetime.now()))
    print("Elapsed Time: " + str(time.process_time()) + " seconds.")
