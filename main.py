# Download data from WDS and update database

import config as cfg  # configuration
import dfhandler as dfh  # for altering pandas data frames
import datetime
import helpers as h  # helper functions
# import pandas as pd
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
        process_date = dt.strftime("%Y-%m-%d")

        # find out which cubes have changed
        print("\nLooking for changed cubes on: " + process_date)
        changed_cubes = wds.get_changed_cube_list(process_date)

        # check the database for matching tables
        products_to_update = db.get_matching_product_list(changed_cubes)
        if len(products_to_update) == 0:
            print("There are no tables to update for " + process_date)  # No tables = No further action
        else:
            print("Found " + str(len(products_to_update)) + " tables to update: " + str(products_to_update))

            products_to_update = [46100027]  # TODO - REMOVE TEST CODE TO RUN ALL PRODUCTS

            # process for each product
            for pid in products_to_update:

                pid_str = str(pid)  # for moments when str is required

                # Download the product tables
                files_downloaded = 0
                pid_path = {"en": {}, "fr": {}}
                for lg in pid_path.keys():
                    pid_path[lg]["Folder"] = WORK_DIR + "\\" + pid_str + "-" + lg
                    pid_path[lg]["CSVFile"] = pid_path[lg]["Folder"] + "\\" + pid_str + ".csv"
                    # TODO - uncomment the code below to download and unzip files (commented out to save time testing)
                    # if wds.get_full_table_download(pid, lg, pid_path[lg]["Folder"] + ".zip"):  # download
                    #     if h.unzip_file(pid_path[lg]["Folder"] + ".zip", pid_path[lg]["Folder"]):  # unzip
                    #         files_downloaded += 1

                files_downloaded = 2  # TODO - REMOVE TEST CODE

                if files_downloaded == len(pid_path):

                    # delete product in database
                    if db.delete_product(pid):

                        # Get the product metadata
                        prod_metadata = wds.get_cube_metadata(pid)
                        dimensions = scwds.get_metadata_dimensions(prod_metadata, True)
                        release_date = scwds.get_metadata_release_date(prod_metadata)

                        # put csv files into data frames and do prelim cleaning
                        df_fr = dfh.load_and_prep_prod_df(pid_path["fr"]["CSVFile"], dimensions, "fr", ";", pid_str,
                                                          release_date)
                        df_en = dfh.load_and_prep_prod_df(pid_path["en"]["CSVFile"], dimensions, "en", ",", pid_str,
                                                          release_date)

                        # start the indicator data frame, then drop french df to save memory
                        df_ind = dfh.start_indicator_df(df_en, df_fr)
                        del df_fr

                        # build remaining indicator columns
                        next_ind_id = db.get_last_indicator_id() + 1  # set unique IDs
                        df_ind = dfh.finish_indicator_df(df_ind, dimensions, next_ind_id)

                        # INSERT TO DB
                        db.insert_indicator(df_ind)

                        # TODO: remove test code
                        df_ind.to_csv(WORK_DIR + "\\" + pid_str + "-testoutput-indicator.csv", encoding='utf-8',
                                      index=False)

                        del df_ind
                        del df_en

    # delete the objects
    db = None
    wds = None

    print("\nScript end: " + str(datetime.datetime.now()))
    print("Elapsed Time: " + str(time.process_time()) + " seconds.")
