# Download updated product data from WDS and update database
import argparse  # for processing arguments passed
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
import sys
import zipfile

WORK_DIR = str(pathlib.Path(__file__).parent.absolute())  # current script path
default_chart_json = WORK_DIR + "\\product_defaults.json"  # default chart info for specific products

# list of crime tables for special handling -- this should be cleaned up later
crime_tables = [35100177, 35100178, 35100179, 35100180, 35100181, 35100182, 35100183, 35100184, 35100185]

# set up logging to file and console
logger = logging.getLogger("etl_log")
logging.basicConfig(format="%(message)s", level=logging.INFO)
file_handler = logging.handlers.RotatingFileHandler(WORK_DIR + "\\etl_log.log", maxBytes=2000000, backupCount=5)
log_fmt = logging.Formatter("%(levelname)s:%(message)s - %(asctime)s")
file_handler.setFormatter(log_fmt)
logger.addHandler(file_handler)  # for writing to file

# look for valid arguments passed to script
parser = argparse.ArgumentParser()
parser.add_argument("--start", type=date.fromisoformat, help="Start date when looking for product updates YYYY-MM-DD")
parser.add_argument("--end",  type=date.fromisoformat, help="End date when looking for product updates YYYY-MM-DD")
parser.add_argument("--prodid", type=int, help="Product ID to update (no special characters)")
args = parser.parse_args()
arg_status = h.check_valid_parse_args(args)
if arg_status != "":
    logger.error(arg_status)
    sys.exit()
start_date = args.start if args.start else False
end_date = args.end if args.end else False
prod_id = args.prodid if args.prodid else False

if __name__ == "__main__":

    logger.info("ETL Process Start: " + str(datetime.now()))

    # create wds and db objects
    wds = scwds.serviceWds(cfg.sc_conn["wds_url"], cfg.sc_conn["delta_url"])
    db = scdb.sqlDb(cfg.sql_conn["driver"], cfg.sql_conn["server"], cfg.sql_conn["database"])

    # create list of products to update
    products_to_update = []
    if start_date and end_date:  # from specified date range
        for dt in h.daterange(start_date, end_date):
            process_date = dt.strftime("%Y-%m-%d")
            changed_cubes = wds.get_changed_cube_list(process_date)  # find out which cubes have changed
            prod_list = db.get_matching_product_list(changed_cubes)  # find out which of these cubes exist in the db
            logger.info("Found " + str(len(prod_list)) + " table(s) to update for " + process_date + ": "
                        + str(prod_list))
            products_to_update.extend(prod_list)
    elif prod_id:  # from specified product
        products_to_update = [prod_id]

    # process for each product
    for pid in products_to_update:

        pid_str = str(pid)  # for moments when str is required
        pid_folder = WORK_DIR + "\\" + pid_str + "-en"
        pid_csv_path = pid_folder + "\\" + pid_str + ".csv"

        is_crime_table = True if int(pid) in crime_tables else False  # crime stats tables have some special handling

        # Download the product tables
        if wds.get_full_table_download(pid, "en", pid_folder + ".zip") and h.valid_zip_file(pid_folder + ".zip"):
            logger.info("Updating Product ID: " + pid_str + "\n")

            # Get any existing product metadata related to charts from the db
            # This is so we can preserve any manual chart configuration that already exists when appending data.
            existing_ind_chart_meta_data = db.get_indicator_chart_info(pid_str)
            # extract the indicator ids from the related charts - TODO
            # existing_ind_chart_meta_data = dfh.add_related_chart_indicator_ids(existing_ind_chart_meta_data)

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
                logger.info("Updating Indicator table.")
                next_ind_id = db.get_last_table_id("IndicatorId", "Indicator", "gis") + 1  # setup unique IDs
                df_ind = dfh.build_indicator_df(pid, release_date, cube_dimensions, wds.uom_codes, ref_dates,
                                                next_ind_id)
                # Indicator data is needed for several other inserts, so send a subset for the insert and keep
                # only those fields needed for next inserts.
                db.insert_dataframe_rows(dfh.build_indicator_df_subset(df_ind), "Indicator", "gis")
                df_ind = df_ind.loc[:, ["IndicatorId", "IndicatorCode", "IndicatorFmt", "UOM_EN", "UOM_FR",
                                        "UOM_ID", "LastIndicatorMember_EN", "LastIndicatorMember_FR"]]
                logger.info("Processed " + f"{df_ind.shape[0]:,}" + " rows for gis.Indicator.\n")

                logger.info("Reading zip file as chunks: " + pid_csv_path + "\n")
                iv_row_count = 0
                gri_row_count = 0
                total_row_count = 0
                geo_levels = []  # for building GeographicLevelforIndicator
                dguid_warnings = []  # for any DGUIDs not found in GeographyReference
                ref_date_dim = []  # keeps track of all references dates for the false "Date" dimension
                logger.info("Updating IndicatorValues and GeographyReferenceForIndicator tables.")
                col_dict = dfh.build_column_and_type_dict(dimensions["en"])  # columns and data types dict

                with zipfile.ZipFile(pid_folder + ".zip") as zf:  # reads in zipped csvas chunks w/o full extraction
                    for csv_chunk in pd.read_csv(zf.open(pid_str + ".csv"), chunksize=20000, sep=",",
                                                 usecols=list(col_dict.keys()),
                                                 dtype=col_dict):  # do not set compression flag - causes badzip error

                        # build formatted cols
                        chunk_data = dfh.setup_chunk_columns(csv_chunk, pid_str, release_date, is_crime_table)

                        # keep unique reference dates for gis.DimensionValues
                        ref_date_chunk = chunk_data.loc[:, ["REF_DATE"]]
                        ref_date_chunk.drop_duplicates(inplace=True)
                        ref_date_dim.append(ref_date_chunk)

                        # keep track of the geographic level for each indicator
                        geo_chunk = chunk_data.loc[:, ["GeographicLevelId", "IndicatorCode"]]
                        geo_levels.append(geo_chunk)

                        # gis.IndicatorValues
                        next_ind_val_id = db.get_last_table_id("IndicatorValueId", "IndicatorValues", "gis") + 1  # IDs
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

                # GeographicLevelforIndicator - from what was built above - feed to next df
                logger.info("\nUpdating GeographicLevelForIndicator table.")
                geo_df = pd.concat(geo_levels)  # puts all the geo_levels dataframes together
                df_gli = dfh.build_geographic_level_for_indicator_df(geo_df, df_ind)
                db.insert_dataframe_rows(df_gli, "GeographicLevelForIndicator", "gis")
                logger.info("Processed " + f"{df_gli.shape[0]:,}" + " rows for gis.GeographicLevelForIndicator.\n")
                del df_gli

                # DimensionValues - from ref_date list created above, add any missing values to false "Date" dimension
                logger.info("Adding new reference dates to DimensionValues table.")
                file_ref_dates_df = pd.concat(ref_date_dim).drop_duplicates(inplace=False)  # combine file ref_dates
                existing_ref_dates_df = db.get_date_dimension_values(pid_str)  # find ref_dates already in db
                date_dimension_id = db.get_date_dimension_id_for_product(pid_str)  # DimensionId for "Date"
                next_dim_val_id = db.get_last_table_id("DimensionValueId", "DimensionValues", "gis") + 1  # next ID
                next_dim_val_display_order = db.get_last_date_dimension_display_order(date_dimension_id) + 1  # next ord
                df_dv = dfh.build_dimension_values_df(file_ref_dates_df, existing_ref_dates_df, date_dimension_id,
                                                      next_dim_val_id, next_dim_val_display_order)
                if df_dv.shape[0] > 0:
                    db.insert_dataframe_rows(df_dv, "DimensionValues", "gis")
                logger.info("Added " + f"{df_dv.shape[0]:,}" + " rows for gis.DimensionValues.\n")
                del df_dv

                # IndicatorMetadata
                logger.info("Updating IndicatorMetadata table.")
                df_dm = db.get_dimensions_and_members_by_product(pid_str)
                df_dim_keys = dfh.build_dimension_unique_keys(df_dm)  # from dimensions/dimensionvalues ids
                df_im = dfh.build_indicator_metadata_df(df_ind, h.get_product_defaults(pid_str, default_chart_json),
                                                        df_dim_keys, existing_ind_chart_meta_data)
                db.insert_dataframe_rows(df_im, "IndicatorMetaData", "gis")
                logger.info("Processed " + f"{df_im.shape[0]:,}" + " rows for gis.IndicatorMetadata.\n")
                del df_im

                # RelatedCharts
                logger.info("Updating RelatedCharts table.")
                df_rc = dfh.build_related_charts_df(df_ind, h.get_product_defaults(pid_str, default_chart_json),
                                                    existing_ind_chart_meta_data)
                db.insert_dataframe_rows(df_rc, "RelatedCharts", "gis")
                logger.info("Processed " + f"{df_rc.shape[0]:,}" + " rows for gis.RelatedCharts.")
                del df_rc

    logger.info("\nETL Process End: " + str(datetime.now()))
