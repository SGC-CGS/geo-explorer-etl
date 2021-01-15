# Download data from WDS and update database

import config as cfg  # configuration
import dfhandler as dfh  # for altering pandas data frames
import datetime
import helpers as h  # helper functions
# import pandas as pd
import pathlib
import scdb  # database class
import scwds  # wds class


# set up
WORK_DIR = str(pathlib.Path(__file__).parent.absolute())  # current script path

# TEST DATE: Oct 28, 2020 has 3 tables from db with updates (46100027, 46100053, 46100054)
#   46100027 - 4,513,250 rows (5 dims)
#   46100053 - 13,869,198 rows (6 dims)
#   46100054 - 13,869,198 rows (6 dims)
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
                        print("Script start: " + str(datetime.datetime.now()) + "\n")
                        # Get the product metadata
                        prod_metadata = wds.get_cube_metadata(pid)
                        dimensions = scwds.get_metadata_dimensions(prod_metadata, True)
                        release_date = scwds.get_metadata_release_date(prod_metadata)

                        # Put csv files into data frames and do prelim cleaning
                        df_fr = dfh.load_and_prep_prod_df(pid_path["fr"]["CSVFile"], dimensions, "fr", ";", pid_str,
                                                          release_date)
                        df_en = dfh.load_and_prep_prod_df(pid_path["en"]["CSVFile"], dimensions, "en", ",", pid_str,
                                                          release_date)

                        # Build and insert datasets for each table
                        # Note data frames are deleted as soon as they are no longer needed to save memory

                        # Indicator
                        next_ind_id = db.get_last_indicator_id() + 1  # setup unique IDs
                        df_ind = dfh.build_indicator_df_start(df_en, df_fr)  # prep first half of df
                        del df_fr
                        df_ind = dfh.build_indicator_df_end(df_ind, dimensions, next_ind_id)  # prep rest of df

                        # This data is needed for several other inserts, so create a subset of just the necessary
                        # columns for the indicator insert so we can keep the rest.
                        df_ind_subset = dfh.build_indicator_df_subset(df_ind)
                        db.insert_dataframe_rows(df_ind_subset, "Indicator", "gis")
                        del df_ind_subset
                        # keep the new IndicatorIds and other required columns for next set of table updates
                        df_ind = df_ind.loc[:, ["IndicatorId", "IndicatorCode", "UOM_EN", "UOM_FR", "UOM_ID"]]

                        # GeographicLevelforIndicator
                        df_gli = dfh.build_geographic_level_for_indicator_df(df_en, df_ind)
                        db.insert_dataframe_rows(df_gli, "GeographicLevelForIndicator", "gis")
                        del df_gli

                        # get ids from gis.GeographyReference for next set of table updates
                        df_geo_ref = db.get_geo_reference_ids()

                        # IndicatorValues
                        next_ind_val_id = db.get_last_indicator_value_id() + 1  # set unique IDs
                        df_ind_null = db.get_indicator_null_reason()  # codes from gis.IndicatorNullReason
                        df_ind_val = dfh.build_indicator_values_df(df_en, df_geo_ref, df_ind_null,
                                                                   next_ind_val_id)
                        db.insert_dataframe_rows(df_ind_val, "IndicatorValues", "gis")
                        # keep the new IndicatorValueIds for next table update
                        df_ind_val.drop(["VALUE", "NullReasonId"], axis=1, inplace=True)
                        del df_ind_null

                        # GeographyReferenceForIndicator
                        df_gri = dfh.build_geography_reference_for_indicator_df(df_en, df_ind, df_geo_ref, df_ind_val)
                        db.insert_dataframe_rows(df_gri, "GeographyReferenceForIndicator", "gis")
                        del df_ind_val
                        del df_geo_ref
                        del df_gri

                        # IndicatorMetadata
                        # TODO - CODE TO BUILD UNIQUE DIMENSION KEYS IS NOT FINISHED.
                        # build dimension key dataset from dimensions/dimensionvalues tables
                        df_dm = db.get_dimensions_and_members_by_product(pid_str)  # get dimensions and members from db

                        df_dim_keys = dfh.build_dimension_keys(df_dm)
                        del df_dm

                        df_im = dfh.build_indicator_metadata_df(
                            df_ind,
                            h.get_product_defaults(pid_str)  # default metadata by product id
                        )
                        db.insert_dataframe_rows(df_im, "IndicatorMetaData", "gis")
                        del df_im
                        del df_ind
                        del df_en

    # delete the objects
    db = None
    wds = None

    print("\nScript end: " + str(datetime.datetime.now()))
