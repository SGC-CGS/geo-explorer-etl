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
                files_done = 0
                prod_path = {"en": {}, "fr": {}}
                for lg in prod_path.keys():
                    prod_path[lg]["Folder"] = WORK_DIR + "\\" + pid_str + "-" + lg
                    # prod_path[lg]["MetaDataFile"] = pid_str + "_MetaData.csv"
                    prod_path[lg]["DataFile"] = pid_str + ".csv"
                    file_ext = ".zip"
                    # if wds.get_full_table_download(pid, lg, prod_path[lg]["Folder"] + file_ext):  # download
                    #     if h.unzip_file(prod_path[lg]["Folder"] + file_ext, prod_path[lg]["Folder"]):  # unzip
                    #         files_done += 1

                files_done = 2  # TODO - REMOVE

                if files_done == len(prod_path):

                    # Keep data in these tables (confirmed):
                    #   Dimensions -- select box on CSGE
                    #   Dimension Values - select box on CSGE
                    #   IndicatorTheme --> product list

                    # Delete data in these tables (in order):
                    #   Indicator
                    #   GeographicLevelForIndicator -- geo level the indicator is available at
                    #   GeographyReferenceForIndicator -- link from indicator value to specific geography
                    #   IndicatorMetaData -- more info about the indicator
                    #   IndicatorValues -- actual values
                    #   RelatedCharts  -- related data charta

                    # TODO - delete existing data, rebuild dataset from csv
                    # delete product in database
                    db.delete_product(pid)

                    # Get the product metadata
                    prod_metadata = wds.get_cube_metadata(pid)
                    dimensions = scwds.get_metadata_dimensions(prod_metadata, True)
                    release_date = scwds.get_metadata_release_date(prod_metadata)

                    # read the file (en) # TODO: read fr file
                    df_en = h.convert_csv_to_df(prod_path["en"]["Folder"] + "\\" + pid_str + ".csv")
                    df = df_en

                    # TODO - move to separate functions by table
                    # Add/remove columns as needed for various tables
                    df["REF_DATE"] = df["REF_DATE"].astype(str)  # 3s
                    # remove ".", correct vintage
                    df["DGUID"] = df["DGUID"].str.replace(".", "").str.replace("201A", "2015A")  # 17s
                    df["RefYear"] = df["REF_DATE"].map(h.fix_ref_year).astype(str)  # need 4 digit year #9s

                    # Build Indicator Code
                    df["TmpCoordinate"] = df["COORDINATE"].str.replace(r"^([^.]+\.)", "",
                                                                       regex=True)  # strips 1st dimension (geography)
                    # IndicatorCode ex. 13100778.1.23.1.2017/2018-01-01
                    df["IndicatorCode"] = pid_str + "." + df["TmpCoordinate"] + "." + df["REF_DATE"] + "-01-01"  # 2s

                    df["IndicatorThemeID"] = pid
                    df["ReleaseIndicatorDate"] = release_date
                    df.rename(columns={"VECTOR": "Vector", "UOM": "UOM_EN"}, inplace=True)
                    df["ReferencePeriod"] = df["RefYear"] + "-01-01"  # becomes Jan 1 of reference year
                    df["Vector"] = df["Vector"].str.replace("v", "")

                    # create df for gis.indicator
                    # remove any duplicates for IndicatorCode since we only need one descriptive row for each
                    df_indicator = df.copy()
                    df_indicator = df_indicator.drop_duplicates(subset=["IndicatorCode"], inplace=False)  # 5350 rows

                    # finish building the rest of the columns on the smaller dataset
                    next_indicator_id = db.get_last_indicator_id() + 1  # set unique IDs
                    df_indicator["IndicatorId"] = pd.RangeIndex(start=next_indicator_id,
                                                                stop=(next_indicator_id + df_indicator.shape[0]))
                    # Concatenate dimension columns
                    # ex. Total, all property types _ 1960 or earlier _ Resident owners only _ Number
                    df_indicator["IndicatorName_EN"] = h.concat_dimension_columns(dimensions["enName"], df_indicator,
                                                                                  " _ ")  # 3s
                    df_indicator["IndicatorDisplay_EN"] = "<ul><li>" + df_indicator["RefYear"] + "<li>" + \
                                                          df_indicator["IndicatorName_EN"].str.replace(" _ ", "<li>") \
                                                          + "</li></ul>"
                    df_indicator["IndicatorNameLong_EN"] = df_indicator["IndicatorName_EN"]  # field is a copy

                    df_indicator["IndicatorName_FR"] = ""
                    df_indicator["IndicatorDisplay_FR"] = ""
                    df_indicator["UOM_FR"] = ""
                    df_indicator["IndicatorNameLong_FR"] = ""

                    # Drop any temporary columns that are no longer needed
                    df_indicator = df_indicator.loc[:, ["IndicatorId", "IndicatorName_EN", "IndicatorName_FR",
                                                        "IndicatorThemeID", "ReleaseIndicatorDate", "ReferencePeriod",
                                                        "IndicatorCode", "IndicatorDisplay_EN", "IndicatorDisplay_FR",
                                                        "UOM_EN", "UOM_FR", "Vector", "IndicatorNameLong_EN",
                                                        "IndicatorNameLong_FR"]]

                    print(df_indicator.head())

                    # check data types before insert to DB

                    # Setup Indicator ID
                    cur_index = db.get_last_indicator_id() + 1

                    # TODO: remove test code
                    df_indicator.to_csv(WORK_DIR + "\\" + pid_str + "-testoutput-indicator.csv", encoding='utf-8',
                                        index=False)  # TEST TODO

                    df_indicator = None
                    df = None

    # delete the objects
    db = None
    wds = None

    print("\nScript end: " + str(datetime.datetime.now()))
    print("Elapsed Time: " + str(time.process_time()) + " seconds.")
