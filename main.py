# Download updated product data from WDS and update database
import config as cfg  # configuration
from datetime import datetime, date
import dfhandler as dfh  # for altering pandas data frames
import helpers as h  # helper functions
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import pathlib
import scdb  # database class
import scwds  # wds class


WORK_DIR = str(pathlib.Path(__file__).parent.absolute())  # current script path

# set up logging to file and console
logger = logging.getLogger("etl_log")
logging.basicConfig(format="%(message)s", level=logging.INFO)
file_handler = logging.handlers.RotatingFileHandler("etl_log.log", maxBytes=2000000, backupCount=5)  # 2MB max 5 backups
log_fmt = logging.Formatter("%(levelname)s:%(message)s")
file_handler.setFormatter(log_fmt)
logger.addHandler(file_handler)  # for writing to file

# TEST DATE: Oct 28, 2020 has 3 tables from db with updates (46100027, 46100053, 46100054)
#   46100027 - 4,513,250 rows (5 dims)
#   46100053 - 13,869,198 rows (6 dims)
#   46100054 - 13,869,198 rows (6 dims)
start_date = date(2020, 10, 27)  # y m d
end_date = date(2020, 10, 28)

if __name__ == "__main__":

    logger.info("ETL Process Start: " + str(datetime.now()))
    logger.info("\nLooking for updates from " + str(start_date) + " to " + str(end_date) + ":")

    # create wds and db objects
    wds = scwds.serviceWds(cfg.sc_conn["wds_url"], cfg.sc_conn["delta_url"])
    db = scdb.sqlDb(cfg.sql_conn["driver"], cfg.sql_conn["server"], cfg.sql_conn["database"])

    products_to_update = []

    # loop through specified date range and create list of cubes to update
    for dt in h.daterange(start_date, end_date):
        process_date = dt.strftime("%Y-%m-%d")
        logger.info("\nLooking for tables to update on " + process_date + ".")
        changed_cubes = wds.get_changed_cube_list(process_date)  # find out which cubes have changed
        prod_list = db.get_matching_product_list(changed_cubes)  # find out which of these cubes exist in the db
        logger.info("There are " + str(len(prod_list)) + " table(s) to update for " + process_date)
        products_to_update.extend(prod_list)

    # process for each product
    for pid in products_to_update:

        pid_str = str(pid)  # for moments when str is required
        pid_folder = WORK_DIR + "\\" + pid_str + "-en"
        pid_csv_path = pid_folder + "\\" + pid_str + ".csv"

        # Download the product tables
        if wds.get_full_table_download(pid, "en", pid_folder + ".zip"):  # download (only need en file)
            if h.unzip_file(pid_folder + ".zip", pid_folder):  # unzip

                # delete product in database
                if db.delete_product(pid):

                    # set up default dates
                    cur_date_fmt = datetime.today().strftime("%Y-%m-%d")  # 2021-01-21
                    cur_date_iso = datetime.today().isoformat(timespec="minutes")  # ex. 2020-12-11T10:11

                    # Get the product metadata
                    prod_metadata = wds.get_cube_metadata(pid)
                    dimensions = scwds.get_metadata_dimension_names(prod_metadata, True)
                    release_date = scwds.get_metadata_field(prod_metadata, "releaseTime", cur_date_iso)
                    cube_start_date = scwds.get_metadata_field(prod_metadata, "cubeStartDate", cur_date_fmt)
                    cube_end_date = scwds.get_metadata_field(prod_metadata, "cubeEndDate", cur_date_fmt)
                    cube_frequency = scwds.get_metadata_field(prod_metadata, "frequencyCode", 12)  # def. annual
                    cube_dimensions = scwds.get_metadata_field(prod_metadata, "dimension", [{}])

                    # reference datasets needed from db
                    df_geo_ref = db.get_geo_reference_ids()  # DGUIDs from gis.GeographyReference
                    df_ind_null = db.get_indicator_null_reason()  # codes from gis.IndicatorNullReason

                    # build list of dates that should be found in the reference data based on the cube frequency
                    ref_dates = dfh.build_reference_date_list(cube_start_date, cube_end_date, cube_frequency)

                    # Indicator
                    next_ind_id = db.get_last_indicator_id() + 1  # setup unique IDs
                    df_ind = dfh.build_indicator_df(pid, release_date, cube_dimensions, wds.uom_codes, ref_dates,
                                                    next_ind_id)
                    # Indicator data is needed for several other inserts, so send a subset for the insert
                    db.insert_dataframe_rows(dfh.build_indicator_df_subset(df_ind), "Indicator", "gis")
                    df_ind = df_ind.loc[:, ["IndicatorId", "IndicatorCode", "IndicatorFmt", "UOM_EN", "UOM_FR",
                                            "UOM_ID"]]  # needed for next round of updates

                    # load csv as chunks
                    logger.info("Reading file as chunks: " + pid_csv_path + "\n")
                    iv_row_count = 0
                    gri_row_count = 0
                    total_row_count = 0
                    geo_levels = []  # for building GeographicLevelforIndicator
                    dguid_warnings = []
                    logger.info("Updating IndicatorValues and GeographyReferenceForIndicator tables.")
                    col_dict = dfh.build_column_and_type_dict(dimensions["en"])  # columns and data types dict
                    for csv_chunk in pd.read_csv(pid_csv_path, chunksize=20000, sep=",", usecols=list(col_dict.keys()),
                                                 dtype=col_dict):

                        # build cols needed
                        chunk_data = dfh.setup_chunk_columns(csv_chunk, pid_str, release_date)

                        # keep track of the geographic level for each indicator
                        geo_chunk = chunk_data.loc[:, ["GeographicLevelId", "IndicatorCode"]]
                        geo_levels.append(geo_chunk)

                        # gis.IndicatorValues
                        next_ind_val_id = db.get_last_indicator_value_id() + 1  # set unique IDs
                        df_ind_val = dfh.build_indicator_values_df(chunk_data, df_geo_ref, df_ind_null, next_ind_val_id)
                        iv_result = db.insert_dataframe_rows(df_ind_val, "IndicatorValues", "gis")
                        df_ind_val.drop(["VALUE", "NullReasonId"], axis=1, inplace=True)  # save for next insert

                        # gis.GeographyReferenceForIndicator - returns data for insert (gri[0]) and warnings (gri[1])
                        gri = dfh.build_geography_reference_for_indicator_df(chunk_data, df_ind, df_geo_ref, df_ind_val)
                        df_gri = gri[0]
                        dguid_warnings.append(gri[1])
                        gri_result = db.insert_dataframe_rows(df_gri, "GeographyReferenceForIndicator", "gis")

                        # update totals
                        total_row_count += chunk_data.shape[0]
                        iv_row_count = (iv_row_count + df_ind_val.shape[0]) if iv_result else iv_row_count
                        gri_row_count = (gri_row_count + df_gri.shape[0]) if gri_result else gri_row_count
                        print("Read " + str(total_row_count) + " rows from file...", end='\r')  # console only

                    # show final counts and any missing DGUIDs
                    logger.info("\nThere were " + f"{total_row_count:,}" + " rows in the file.")
                    logger.info("Processed " + f"{iv_row_count:,}" + " rows for gis.IndicatorValues.")
                    logger.info("Processed " + f"{gri_row_count:,}" + " rows for gis.GeographyReferenceForIndicator.")
                    logger.warning(dfh.write_dguid_warning(pd.concat(dguid_warnings)))  # concat warnings df list first

                    # GeographicLevelforIndicator - from what was built above - should have a df you can feed in
                    geo_df = pd.concat(geo_levels)  # puts all the geo_levels dataframes together
                    logger.info("\nUpdating GeographicLevelForIndicator table.")
                    df_gli = dfh.build_geographic_level_for_indicator_df(geo_df, df_ind)
                    db.insert_dataframe_rows(df_gli, "GeographicLevelForIndicator", "gis")
                    logger.info("Processed " + f"{df_gli.shape[0]:,}" + " rows for gis.GeographicLevelForIndicator.\n")

                    # IndicatorMetadata - defaults come from product_defaults.json
                    logger.info("Updating IndicatorMetadata table.")
                    df_dm = db.get_dimensions_and_members_by_product(pid_str)
                    df_dim_keys = dfh.build_dimension_unique_keys(df_dm)  # from dimensions/dimensionvalues ids
                    df_im = dfh.build_indicator_metadata_df(df_ind, h.get_product_defaults(pid_str), df_dim_keys)
                    db.insert_dataframe_rows(df_im, "IndicatorMetaData", "gis")
                    logger.info("Processed " + f"{df_im.shape[0]:,}" + " rows for gis.IndicatorMetadata.")

    logger.info("\nETL Process End: " + str(datetime.now()))
