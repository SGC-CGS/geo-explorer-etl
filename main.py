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
langs = ["en", "fr"]

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

            products_to_update = [46100027]  # TODO - remove test code

            # process for each product
            for pid in products_to_update:

                pid_str = str(pid)  # for moments when str is required

                # Download the product tables
                files_downloaded = 0
                pid_path = {"en": {}, "fr": {}}
                for lg in pid_path.keys():
                    pid_path[lg]["Folder"] = WORK_DIR + "\\" + pid_str + "-" + lg
                    pid_path[lg]["DataFile"] = pid_str + ".csv"
                    file_ext = ".zip"
                    # if wds.get_full_table_download(pid, lg, pid_path[lg]["Folder"] + file_ext):  # download
                    #     if h.unzip_file(pid_path[lg]["Folder"] + file_ext, pid_path[lg]["Folder"]):  # unzip
                    #         files_done += 1

                files_downloaded = 2  # TODO - REMOVE

                if files_downloaded == len(pid_path):

                    # delete product in database
                    if db.delete_product(pid):

                        # Get the product metadata
                        prod_metadata = wds.get_cube_metadata(pid)
                        dimensions = scwds.get_metadata_dimensions(prod_metadata, True)
                        release_date = scwds.get_metadata_release_date(prod_metadata)

                        fr_csv = pid_path["fr"]["Folder"] + "\\" + pid_str + ".csv"
                        en_csv = pid_path["en"]["Folder"] + "\\" + pid_str + ".csv"

                        # build dict of columns and types
                        cols = h.build_column_and_type_dict(dimensions, langs)

                        # process french dataset first so we can reduce the mem footprint quickly
                        df_fr = h.convert_csv_to_df(fr_csv, ";", cols["fr"])
                        df_fr["IndicatorCode"] = h.build_indicator_code(df_fr["COORDONNÉES"],
                                                                        df_fr["PÉRIODE DE RÉFÉRENCE"],
                                                                        pid_str)
                        df_fr.drop(["COORDONNÉES"], axis=1, inplace=True)
                        df_fr.drop_duplicates(subset=["IndicatorCode"], inplace=True)  # 5350
                        df_fr.rename(columns={"UNITÉ DE MESURE": "UOM_FR"}, inplace=True)

                        # load english dataset (largest)
                        df_en = h.convert_csv_to_df(en_csv, ",", cols["en"])
                        df_en["IndicatorCode"] = h.build_indicator_code(df_en["COORDINATE"], df_en["REF_DATE"], pid_str)
                        df_en.drop(["COORDINATE"], axis=1, inplace=True)
                        df_en.rename(columns={"VECTOR": "Vector", "UOM": "UOM_EN"}, inplace=True)

                        # preliminary data preparation
                        df_en["DGUID"] = df_en["DGUID"].str.replace(".", "").str.replace("201A", "2015A")  # fix vintage
                        df_en["RefYear"] = df_en["REF_DATE"].map(h.fix_ref_year).astype("string")  # need 4 digit year
                        df_en["IndicatorThemeID"] = pid
                        df_en["ReleaseIndicatorDate"] = release_date
                        df_en["ReferencePeriod"] = df_en["RefYear"] + "-01-01"
                        df_en["ReferencePeriod"] = df_en["ReferencePeriod"].astype("datetime64[ns]")  # becomes Jan 1
                        df_en["Vector"] = df_en["Vector"].str.replace("v", "").astype("int32")  # remove v, convert int

                        # create df for gis.indicator
                        print("Building Indicator Table...")
                        # remove any duplicates for IndicatorCode since we only need one descriptive row for each
                        df_en_ind = df_en.drop_duplicates(subset=["IndicatorCode"], inplace=False)  # 5350
                        df_ind = pd.merge(df_en_ind, df_fr, on="IndicatorCode")
                        df_en_ind = None
                        df_fr = None

                        # build remaining indicator columns
                        next_ind_id = db.get_last_indicator_id() + 1  # set unique IDs
                        df_ind["IndicatorId"] = pd.RangeIndex(start=next_ind_id, stop=(next_ind_id + df_ind.shape[0]))

                        for lang in langs:
                            for dim in dimensions[lang]:  # convert dimensions to string for concatenation
                                df_ind[dim] = df_ind[dim].astype("string")

                        # Concatenate dimension columns
                        # ex. Total, all property types _ 1960 or earlier _ Resident owners only _ Number
                        df_ind["IndicatorName_EN"] = h.concat_dimension_cols(dimensions["en"], df_ind, " _ ")
                        df_ind["IndicatorDisplay_EN"] = h.build_dimension_ul(df_ind["RefYear"],
                                                                             df_ind["IndicatorName_EN"])
                        df_ind["IndicatorNameLong_EN"] = df_ind["IndicatorName_EN"]  # copy - save for db update

                        df_ind["IndicatorName_FR"] = h.concat_dimension_cols(dimensions["fr"], df_ind, " _ ")
                        df_ind["IndicatorDisplay_FR"] = h.build_dimension_ul(df_ind["RefYear"],
                                                                             df_ind["IndicatorName_FR"])
                        df_ind["IndicatorNameLong_FR"] = df_ind["IndicatorName_FR"]  # copy - save for db update

                        # Keep only the columns needed for insert
                        df_ind = df_ind.loc[:, ["IndicatorId", "IndicatorName_EN", "IndicatorName_FR",
                                                "IndicatorThemeID", "ReleaseIndicatorDate", "ReferencePeriod",
                                                "IndicatorCode", "IndicatorDisplay_EN", "IndicatorDisplay_FR", "UOM_EN",
                                                "UOM_FR", "Vector", "IndicatorNameLong_EN", "IndicatorNameLong_FR"]]

                        # Fix any data types and field lengths before insert to DB
                        df_ind["IndicatorName_EN"] = df_ind["IndicatorName_EN"].str[:1000]  # str
                        df_ind["IndicatorName_FR"] = df_ind["IndicatorName_FR"].str[:1000]  # str
                        df_ind["ReleaseIndicatorDate"] = df_ind["ReleaseIndicatorDate"].astype("datetime64[ns]")
                        df_ind["IndicatorCode"] = df_ind["IndicatorCode"].str[:100]  # str
                        df_ind["IndicatorDisplay_EN"] = df_ind["IndicatorDisplay_EN"].str[:500]  # str
                        df_ind["IndicatorDisplay_FR"] = df_ind["IndicatorDisplay_FR"].str[:500]  # str
                        df_ind["UOM_EN"] = df_ind["UOM_EN"].astype("string").str[:50]  # str
                        df_ind["UOM_FR"] = df_ind["UOM_FR"].astype("string").str[:50]  # str
                        df_ind["IndicatorNameLong_EN"] = df_ind["IndicatorNameLong_EN"].str[:1000]  # str
                        df_ind["IndicatorNameLong_FR"] = df_ind["IndicatorNameLong_FR"].str[:1000]  # str

                        # INSERT TO DB
                        db.insert_indicator(df_ind)

                        # TODO: remove test code
                        df_ind.to_csv(WORK_DIR + "\\" + pid_str + "-testoutput-indicator.csv", encoding='utf-8',
                                      index=False)

                        df_ind = None
                        df_en = None

    # delete the objects
    db = None
    wds = None

    print("\nScript end: " + str(datetime.datetime.now()))
    print("Elapsed Time: " + str(time.process_time()) + " seconds.")
