# Download updated product data from WDS and update database
import config as cfg  # configuration
from datetime import datetime, date
import dfhandler as dfh  # for altering pandas data frames
import helpers as h  # helper functions
import logging
from logging.handlers import RotatingFileHandler
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
    logger.info("Looking for updates from " + str(start_date) + " to " + str(end_date) + ":")

    # create wds and db objects
    wds = scwds.serviceWds(cfg.sc_conn["wds_url"], cfg.sc_conn["delta_url"])
    db = scdb.sqlDb(cfg.sql_conn["driver"], cfg.sql_conn["server"], cfg.sql_conn["database"])

    # loop through specified date range
    for dt in h.daterange(start_date, end_date):
        process_date = dt.strftime("%Y-%m-%d")

        # find out which cubes have changed
        logger.info("\nLooking for changed cubes on: " + process_date)
        changed_cubes = wds.get_changed_cube_list(process_date)

        # check the database for matching tables
        products_to_update = db.get_matching_product_list(changed_cubes)
        if len(products_to_update) == 0:
            logger.info("There are no tables to update for " + process_date)  # No tables = No further action
        else:
            logger.info("Found " + str(len(products_to_update)) + " tables to update: " + str(products_to_update))

            products_to_update = [46100027]  # TODO - REMOVE TEST CODE TO RUN ALL PRODUCTS

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

                            # Get the product metadata
                            prod_metadata = wds.get_cube_metadata(pid)
                            dimensions = scwds.get_metadata_dimension_names(prod_metadata, True)
                            release_date = scwds.get_metadata_release_date(prod_metadata)
                            cube_start_date = scwds.get_metadata_field(prod_metadata, "cubeStartDate", cur_date_fmt)
                            cube_end_date = scwds.get_metadata_field(prod_metadata, "cubeEndDate", cur_date_fmt)
                            cube_frequency = scwds.get_metadata_field(prod_metadata, "frequencyCode", 12)  # def. annual
                            cube_dimensions = scwds.get_metadata_field(prod_metadata, "dimension", [{}])
                            uom_codes = wds.uom_codes

                            # build list of dates that should be found in the reference data based on the cube frequency
                            ref_dates = dfh.build_reference_date_list(cube_start_date, cube_end_date, cube_frequency)

                            # Indicator
                            next_ind_id = db.get_last_indicator_id() + 1  # setup unique IDs
                            df_ind = dfh.build_indicator_df(pid, release_date, cube_dimensions, uom_codes, ref_dates,
                                                            next_ind_id)

                            # This data is needed for several other inserts, so create a subset of just the necessary
                            # columns for the indicator insert so we can keep the rest.
                            df_ind_subset = dfh.build_indicator_df_subset(df_ind)
                            db.insert_dataframe_rows(df_ind_subset, "Indicator", "gis")
                            del df_ind_subset
                            # keep the new IndicatorIds and other required columns for next set of table updates
                            df_ind = df_ind.loc[:, ["IndicatorId", "IndicatorCode", "IndicatorFmt",
                                                    "UOM_EN", "UOM_FR", "UOM_ID"]]

                            # Put csv files into data frames and do prelim cleaning
                            df_en = dfh.load_and_prep_prod_df(pid_csv_path, dimensions, "en", ",", pid_str,
                                                              release_date)

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
                            df_gri = dfh.build_geography_reference_for_indicator_df(df_en, df_ind, df_geo_ref,
                                                                                    df_ind_val)
                            db.insert_dataframe_rows(df_gri, "GeographyReferenceForIndicator", "gis")
                            del df_ind_val
                            del df_geo_ref
                            del df_gri

                            # IndicatorMetadata
                            # build dimension key dataset from dimensions/dimensionvalues tables
                            df_dm = db.get_dimensions_and_members_by_product(pid_str)
                            df_dim_keys = dfh.build_dimension_unique_keys(df_dm)
                            del df_dm

                            df_im = dfh.build_indicator_metadata_df(
                                df_ind,
                                h.get_product_defaults(pid_str),  # default metadata by product id
                                df_dim_keys
                            )
                            db.insert_dataframe_rows(df_im, "IndicatorMetaData", "gis")
                            del df_im
                            del df_ind
                            del df_en

    # delete the objects
    db = None
    wds = None

    logger.info("ETL Process End: " + str(datetime.now()))
