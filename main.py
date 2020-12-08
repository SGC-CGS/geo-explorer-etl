# Download data from WDS and update database

import config as cfg  # configuration
import datetime
import helpers as h  # helper functions
import pandas as pd
import pathlib
import scdb  # database class
import scwds  # wds class
import time

# set up
WORK_DIR = str(pathlib.Path(__file__).parent.absolute())  # current script path

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

            products_to_update = [46100027]  # TODO - remove test code

            # process for each product
            for pid in products_to_update:

                # Download the tables
                files_done = 0
                prod_path = {"en": {}, "fr": {}}
                for lg in prod_path.keys():
                    prod_path[lg]["Folder"] = WORK_DIR + "\\" + str(pid) + "-" + lg
                    prod_path[lg]["MetaDataFile"] = str(pid) + "_MetaData.csv"
                    prod_path[lg]["DataFile"] = str(pid) + ".csv"
                    file_ext = ".zip"
                    if wds.get_full_table_download(pid, lg, prod_path[lg]["Folder"] + file_ext):  # download
                        if h.unzip_file(prod_path[lg]["Folder"] + file_ext, prod_path[lg]["Folder"]):  # unzip
                            files_done += 1

                if files_done == len(prod_path):

                    # TODO - delete existing data, rebuild dataset from csv
                    

                    # read the file (en) # TODO: read fr file
                    prod_rows = []
                    print("Reading full table")
                    for chunk in pd.read_csv(prod_path["en"]["Folder"] + "\\" + str(pid) + ".csv", chunksize=10000):
                        prod_rows.append(chunk)

                    prod_df = pd.concat(prod_rows)  # add all DGUIDs/vectors to data frame
                    prod_rows = None
                    print(prod_df.head())
                    prod_df = None

    # delete the objects
    db = None
    wds = None

    print("\nScript end: " + str(datetime.datetime.now()))
    print("Elapsed Time: " + str(time.process_time()) + " seconds.")
