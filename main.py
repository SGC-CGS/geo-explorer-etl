# Download updated product data from WDS and update database
import arguments  # for parsing CLI arguments
import config as cfg  # configuration
from datetime import datetime
import dfhandler as dfh  # for altering pandas data frames
import helpers as h  # helper functions
import json_handler as jh
import pandas as pd
import pathlib
import scdb  # database class
import scwds  # wds class
import zipfile

WORK_DIR = str(pathlib.Path(__file__).parent.absolute())  # current script path
default_chart_json = WORK_DIR + "\\product_defaults.json"  # default chart info for specific products
products_to_merge_json = WORK_DIR + "\\products_to_merge.json"  # products to be merged to a single IndicatorThemeID

# Products w/ mixed geographies need special handling of reference periods (can/prov/region - all data, others 2017+)
# Note only the master product id is included here when it is a merged product. TODO --> find a cleaner way to do this
mixed_geo_justice_pids = [35100177, 35100002, 35100026, 35100068]

logger = h.setup_logger(WORK_DIR, "etl_log")  # set up logging to file and console

arg = arguments.argParser()  # get CLI arguments
arg_status = arg.check_valid_parse_args()
if arg_status != "":
    arg.show_help_and_exit_with_msg("\nArgument Error: " + arg_status)
start_date = arg.get_arg_value("start")
end_date = arg.get_arg_value("end")
prod_id = arg.get_arg_value("prodid")
insert_new_table = arg.get_arg_value("insert_new_table")
min_ref_year = arg.get_arg_value("minrefyear")

if __name__ == "__main__":
    ###########################################################
    # SETUP
    logger.info("ETL Process Start: " + str(datetime.now()))

    wds = scwds.serviceWds(cfg.sc_conn["wds_url"], cfg.sc_conn["delta_url"])  # set up web services
    db = scdb.sqlDb(cfg.sql_conn["driver"], cfg.sql_conn["server"], cfg.sql_conn["database"])  # set up db

    existing_prod_ids = db.get_matching_product_list(prod_id)  # check whether product already exists in db
    if len(existing_prod_ids) > 0 and insert_new_table:
        arg.show_help_and_exit_with_msg("\nCannot insert product because one or more Product IDs already exist in "
                                        "gis.IndicatorTheme/gis.Dimensions. Run without -i to append data. " +
                                        str(existing_prod_ids))
    elif len(existing_prod_ids) == 0 and prod_id and not insert_new_table:
        arg.show_help_and_exit_with_msg("\nCannot append Product ID because it does not exist in gis.IndicatorTheme. "
                                        "Run with -i to add a new product. " + str(prod_id))

    ###########################################################
    # INSERT - runs when -i flag is present
    if insert_new_table:
        ind_theme_id = prod_id[0] if prod_id[0] else ""  # 1st product id given will be inserted as main product
        if len(prod_id) > 1:  # if it is a table to be merged, update the json file for merged tables
            jh.update_merge_products_json(ind_theme_id, prod_id, products_to_merge_json)

        new_merged_prods = jh.get_merged_tables_from_json(products_to_merge_json)
        if len(prod_id) == 1 and jh.is_master_in_merged_product(ind_theme_id, new_merged_prods):
            # notify user if they tried to insert a table that has already been flagged as a master in json
            arg.show_help_and_exit_with_msg(arguments.show_merge_warning("", ind_theme_id, products_to_merge_json))
        elif len(prod_id) == 1 and jh.is_sibling_in_merged_product(ind_theme_id, new_merged_prods):
            # notify user if they tried to insert a table that has already been flagged as a sibling in json
            arg.show_help_and_exit_with_msg(
                arguments.show_merge_warning(ind_theme_id, jh.get_master_prod_id(ind_theme_id, new_merged_prods),
                                                     products_to_merge_json))
        else:
            pid_meta = scwds.build_metadata_dict(wds.get_cube_metadata(ind_theme_id), ind_theme_id)  # product metadata
            ex_subj = db.get_matching_product_list([pid_meta["subject_code"]])  # existing 2-5 digit subject code
            ex_subj_short = db.get_matching_product_list([pid_meta["subject_code_short"]])  # existing subject code (2)
            ex_subj_dummy = db.get_matching_product_list(
                [str(pid_meta["subject_code"]) + h.create_dummy_subject_code_suffix(pid_meta["subject_code"])])
            ex_subj_short_dummy = db.get_matching_product_list(
                [str(pid_meta["subject_code_short"]) +
                 h.create_dummy_subject_code_suffix(pid_meta["subject_code_short"])])

            # insert to gis.IndicatorTheme
            logger.info("Adding product to IndicatorTheme table.")
            df_ind_theme = dfh.build_indicator_theme_df(pid_meta, ind_theme_id, ex_subj, ex_subj_short, ex_subj_dummy,
                                                        ex_subj_short_dummy, wds.subject_codes)
            it_result = db.insert_dataframe_rows(df_ind_theme, "IndicatorTheme", "gis")
            h.delete_var_and_release_mem([df_ind_theme])

            # insert to gis.Dimensions
            logger.info("Adding product to Dimensions table.")
            next_dim_id = db.get_last_table_id("DimensionId", "Dimensions", "gis") + 1  # setup unique IDs
            df_dims = dfh.build_dimension_df(pid_meta, ind_theme_id, next_dim_id)
            dim_result = db.insert_dataframe_rows(df_dims, "Dimensions", "gis")

            # insert to gis.DimensionValues (Note: Geo is dropped and "Date" dimension will be added during append)
            logger.info("Adding product to DimensionValues table.\n")
            next_dim_val_id = db.get_last_table_id("DimensionValueId", "DimensionValues", "gis") + 1  # setup unique IDs
            df_dim_vals = dfh.build_dimension_values_df(pid_meta, df_dims, next_dim_val_id)
            dim_val_result = db.insert_dataframe_rows(df_dim_vals, "DimensionValues", "gis")
            h.delete_var_and_release_mem([df_dims, df_dim_vals])

    ###########################################################
    # APPEND - runs whether inserting or updating a table
    merged_prod_dict = jh.get_merged_tables_from_json(products_to_merge_json)  # find info about any merged tables
    products_to_update = []  # create list of products to be updated

    if start_date and end_date:
        # update products for specified date range - this section only executes if --start and --end args are present
        for dt in h.daterange(start_date, end_date):
            changed_cubes = wds.get_changed_cube_list(dt.strftime("%Y-%m-%d"))  # find out which cubes have changed
            prod_list = db.get_matching_product_list(changed_cubes)  # find out which of these cubes exist in the db
            logger.info(str(len(prod_list)) + " table(s) found for " + dt.strftime("%Y-%m-%d") + ": " + str(prod_list))
            products_to_update.extend(prod_list)

        # if any product in the changed cube list is part of a merged product, remove the prodid from the list to be
        # processed and notify the user that they will have to update that product separately
        check_pids = products_to_update.copy()
        for check_pid in check_pids:
            if jh.is_master_in_merged_product(check_pid, merged_prod_dict):
                logger.warning(arguments.show_merge_warning("", check_pid, products_to_merge_json))
                products_to_update.remove(check_pid)
            elif jh.is_sibling_in_merged_product(check_pid, merged_prod_dict):
                logger.warning(
                    arguments.show_merge_warning(check_pid, jh.get_master_prod_id(check_pid, merged_prod_dict),
                                                         products_to_merge_json))
                products_to_update.remove(check_pid)

    elif prod_id:
        # update specified product from --prodid arg
        products_to_update = prod_id
        # If > 1 prod_id: indicates a new merged table, all sibling product ids will already be included in prod_id arg.
        # If single specified product is a master table for append, find all siblings and save to products_to_update.
        if len(prod_id) == 1 and jh.is_master_in_merged_product(prod_id[0], merged_prod_dict):
            sibling_pids = (jh.get_sibling_prod_ids(prod_id[0], merged_prod_dict))
            products_to_update = h.combine_ordered_lists(products_to_update, sibling_pids)  # ensures master runs 1st

    # run append on each product to be updated
    for pid in products_to_update:
        pid_str = str(pid)  # for moments when str is required
        pid_folder = WORK_DIR + "\\" + pid_str + "-en"
        pid_csv_path = pid_folder + "\\" + pid_str + ".csv"

        # Check if product is a master or sibling table (could be neither). Determines which db tables get updated.
        is_master = jh.is_master_in_merged_product(pid, merged_prod_dict)
        is_sibling = jh.is_sibling_in_merged_product(pid, merged_prod_dict)
        master_pid_str = jh.get_master_prod_id(pid, merged_prod_dict) if is_sibling else ""
        functional_pid_str = master_pid_str if is_sibling else pid_str  # pid that will be saved to db for this product

        # Download the product
        if wds.get_full_table_download(pid, "en", pid_folder + ".zip") and h.valid_zip_file(pid_folder + ".zip"):
            if is_sibling:
                logger.info("Updating sibling Product ID: " + pid_str + " (Master ID: " + master_pid_str + ").")
            elif is_master:
                logger.info("Updating master Product ID: " + pid_str + ". Sibling product updates will follow.")
            else:
                logger.info("Updating Product ID: " + pid_str + "\n")

            # keep any existing product chart info to preserve some of the manual chart diplay configuration if possible
            existing_ind_chart_meta_data = db.get_indicator_chart_info(pid_str)

            # delete product in database (only if not a sibling product)
            if db.delete_product(pid, is_sibling):
                pid_meta = scwds.build_metadata_dict(wds.get_cube_metadata(pid), pid_str)  # product metadata
                df_geo_ref = db.get_geo_reference_ids()  # DGUIDs from gis.GeographyReference
                df_ind_null = db.get_indicator_null_reason()  # codes from gis.IndicatorNullReason

                # build list of dates that should be found in the reference data based on the cube frequency
                ref_dates = dfh.build_reference_dates(pid_meta["start_date"], pid_meta["end_date"], pid_meta["freq"])

                # Indicator
                if is_sibling:
                    # for sibling tables, need to retrieve master indicator info from db.
                    logger.info("Retrieving Indicator information from master product.")
                    df_ind = db.get_indicators(master_pid_str)
                    df_ind = df_ind.loc[:, ["IndicatorId", "IndicatorCode", "UOM_EN", "UOM_FR"]]
                else:
                    logger.info("Updating Indicator table.")
                    next_ind_id = db.get_last_table_id("IndicatorId", "Indicator", "gis") + 1  # setup unique IDs
                    df_ind = dfh.build_indicator_df(pid, pid_meta["release_date"], pid_meta["dimensions_and_members"],
                                                    wds.uom_codes, ref_dates, next_ind_id, min_ref_year,
                                                    mixed_geo_justice_pids)
                    # subset for insert and keep only fields needed for next table inserts.
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
                col_dict = dfh.build_column_and_type_dict(pid_meta["dimension_names"]["en"])  # column/data type dict

                with zipfile.ZipFile(pid_folder + ".zip") as zf:  # reads in zipped csvas chunks w/o full extraction
                    for csv_chunk in pd.read_csv(zf.open(pid_str + ".csv"), chunksize=20000, sep=",",
                                                 usecols=list(col_dict.keys()), dtype=col_dict):  # NO compression flag

                        # build formatted cols - sibling tables will be saved under the master product id
                        chunk_data = dfh.setup_chunk_columns(csv_chunk, functional_pid_str, pid_meta["release_date"],
                                                                 min_ref_year, mixed_geo_justice_pids)

                        # keep unique reference dates for gis.DimensionValues
                        ref_date_chunk = chunk_data.loc[:, ["REF_DATE", "RefYear", "GeographicLevelId"]]
                        ref_date_dim.append(dfh.build_ref_date_dimensions(ref_date_chunk, min_ref_year,
                                                                          functional_pid_str, mixed_geo_justice_pids))

                        # keep track of the geographic level for each indicator
                        geo_levels.append(dfh.build_geographic_level_chunk_df(chunk_data, functional_pid_str,
                                                                              mixed_geo_justice_pids))

                        # gis.IndicatorValues
                        next_ind_val_id = db.get_last_table_id("IndicatorValueId", "IndicatorValues", "gis") + 1  # IDs
                        df_ind_val = dfh.build_indicator_values_df(chunk_data, df_geo_ref, df_ind_null,
                                                                   next_ind_val_id, functional_pid_str,
                                                                   mixed_geo_justice_pids, is_sibling)
                        iv_result = db.insert_dataframe_rows(df_ind_val, "IndicatorValues", "gis")
                        df_ind_val.drop(["VALUE", "NullReasonId", "FormattedValue_EN", "FormattedValue_FR"], axis=1,
                                        inplace=True)  # save for next insert

                        # gis.GeographyReferenceForIndicator - returns data for insert (gri[0]) and warnings (gri[1])
                        gri = dfh.build_geography_reference_for_indicator_df(chunk_data, df_ind, df_geo_ref, df_ind_val)
                        df_gri = gri[0]
                        dguid_warnings.append(gri[1])
                        gri_result = db.insert_dataframe_rows(df_gri, "GeographyReferenceForIndicator", "gis")

                        # update totals
                        total_row_count += chunk_data.shape[0]
                        iv_row_count = (iv_row_count + df_ind_val.shape[0]) if iv_result else iv_row_count
                        gri_row_count = (gri_row_count + df_gri.shape[0]) if gri_result else gri_row_count
                        print("Loading " + str(total_row_count) + " rows from file...", end='\r')  # console only

                # show final counts and any missing DGUIDs
                logger.info("\nThere were " + f"{total_row_count:,}" + " rows in the file.")
                logger.info("Processed " + f"{iv_row_count:,}" + " rows for gis.IndicatorValues.")
                logger.info("Processed " + f"{gri_row_count:,}" + " rows for gis.GeographyReferenceForIndicator.")
                logger.warning(dfh.write_dguid_warning(pd.concat(dguid_warnings)))  # concat warnings df list first

                # GeographicLevelforIndicator - from what was built above feed next to df
                logger.info("\nUpdating GeographicLevelForIndicator table.")
                geo_df = pd.concat(geo_levels)  # puts all the geo_levels dataframes together
                existing_geo_levels_df = db.get_geo_levels(functional_pid_str)

                df_gli = dfh.build_geographic_level_for_indicator_df(geo_df, df_ind, existing_geo_levels_df, is_sibling)
                db.insert_dataframe_rows(df_gli, "GeographicLevelForIndicator", "gis")
                logger.info("Processed " + f"{df_gli.shape[0]:,}" + " rows for gis.GeographicLevelForIndicator.\n")
                h.delete_var_and_release_mem([df_gli])

                # DimensionValues - from ref_date list created above, add any missing values to false "Date" dimension
                logger.info("Adding new reference dates to DimensionValues table.")
                file_ref_dates_df = pd.concat(ref_date_dim).drop_duplicates(inplace=False)  # combine file ref_dates
                # find ref_dates already in db under the DimensionId for "Date" - siblings use master id
                existing_ref_dates_df = db.get_date_dimension_values(functional_pid_str)
                date_dimension_id = db.get_date_dimension_id_for_product(functional_pid_str)
                next_dim_val_id = db.get_last_table_id("DimensionValueId", "DimensionValues", "gis") + 1  # next ID
                next_dim_val_display_order = db.get_last_date_dimension_display_order(date_dimension_id) + 1  # next ord
                df_dv = dfh.build_date_dimension_values_df(file_ref_dates_df, existing_ref_dates_df, date_dimension_id,
                                                           next_dim_val_id, next_dim_val_display_order)
                if df_dv.shape[0] > 0:
                    db.insert_dataframe_rows(df_dv, "DimensionValues", "gis")
                logger.info("Added " + f"{df_dv.shape[0]:,}" + " row(s) for gis.DimensionValues.\n")
                h.delete_var_and_release_mem([df_dv])

                if not is_sibling:  # (master or single tables only)
                    # IndicatorMetadata
                    logger.info("Updating IndicatorMetadata table.")
                    df_dm = db.get_dimensions_and_members_by_product(pid_str)
                    df_dim_keys = dfh.build_dimension_unique_keys(df_dm)  # from dimensions/dimensionvalues ids
                    df_im = dfh.build_indicator_metadata_df(df_ind,
                                                            jh.get_product_defaults(pid_str, default_chart_json),
                                                            df_dim_keys, existing_ind_chart_meta_data)
                    db.insert_dataframe_rows(df_im, "IndicatorMetaData", "gis")
                    logger.info("Processed " + f"{df_im.shape[0]:,}" + " rows for gis.IndicatorMetadata.\n")
                    h.delete_var_and_release_mem([df_im])

                    # RelatedCharts
                    logger.info("Updating RelatedCharts table.")
                    df_rc = dfh.build_related_charts_df(df_ind, jh.get_product_defaults(pid_str, default_chart_json),
                                                        existing_ind_chart_meta_data)
                    db.insert_dataframe_rows(df_rc, "RelatedCharts", "gis")
                    logger.info("Processed " + f"{df_rc.shape[0]:,}" + " rows for gis.RelatedCharts.\n")
                    h.delete_var_and_release_mem([df_rc])

                logger.info("Finished processing product: " + pid_str + "\n")

    logger.info("\nETL Process End: " + str(datetime.now()))
