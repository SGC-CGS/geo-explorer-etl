# data frame handling
from datetime import datetime
import helpers as h  # helper functions
import itertools as it  # for iterators
import logging
import numpy as np
import pandas as pd
import re  # regular expressions

# set up logger if available
log = logging.getLogger("etl_log")
log.addHandler(logging.NullHandler())


def add_related_chart_indicator_ids(rcdf):  # TODO -- this function is incomplete
    # for a dataframe with related chart data (rcdf), extract the indicator ids from the "Query" column.
    # These indicator ids will be added as a list in a new column and a new df is returned.
    new_rc_df = rcdf
    # example code: (from another proc, need to update this.)
    # "02. Resident owners only" -->removes "02. "
    # new_rc_df["newcol"] = re.sub(r"^([^.]+\.)", "", row["Display_EN"]).lstrip()

    return new_rc_df


def build_column_and_type_dict(dimensions):
    # set up the dicionary of columns and data types for pandas df, then add columns listed in dimensions as str types
    # Note: All strings as object type by default. Categories are more efficient for string fields if there are < 50%
    # unique values and no string operations are required.
    cols = {"REF_DATE": "string", "DGUID": "string", "UOM": "category", "UOM_ID": "int16", "VECTOR": "string",
            "COORDINATE": "string", "STATUS": "category", "SYMBOL": "string", "VALUE": "float64"}
    for dim in dimensions:
        cols[dim] = "string"
    return cols


def build_dimension_unique_keys(dmf):
    # Build a dataframe of unique dimension keys for the product id (pid) from a dataset returned from gis.Dimensions
    # and gis.DimensionValues (dmf). The unique keys are the ordered and concatenated index values of each member in
    # gis.DimensionValues. There are no IndicatorIds, vectors, or coordinates in these tables, so we are figuring out
    # the link to Indicator backward through reference periods and indicator names.
    dim_mem_names = {}
    dim_mem_ids = {}

    # build dictionaries of member ids and member names
    for index, row in dmf.iterrows():
        dim_id = row["DimensionId"]
        mem_id = row["DimensionValueId"]
        # regex below handles cases where a sorting prefix has been added to a dimension/member
        patt = r"^(?:(?:0){0,3}[0-9]|(?:0){0,2}[1-9][0-9]|(?:0){0,1}[1-9][0-9][0-9])\."  # regex match for 0. to 1000.
        mem_name = re.sub(patt, "", row["Display_EN"]).lstrip()  # "02. Resident owners only" -->removes "02. "

        if dim_id not in dim_mem_names:
            dim_mem_names[dim_id] = []
        dim_mem_names[dim_id].append(mem_name)
        if dim_id not in dim_mem_ids:
            dim_mem_ids[dim_id] = []
        dim_mem_ids[dim_id].append(mem_id)

    mem_names = build_dimension_member_combos(dim_mem_names, "-")
    mem_ids = build_dimension_member_combos(dim_mem_ids, "-")
    keys_df = False
    if len(mem_names) == len(mem_ids):
        # can combine the two lists b/c they are in the same order by dimension
        keys_df = pd.DataFrame({"IndicatorFmt": mem_names, "DimensionUniqueKey": mem_ids})
    return keys_df


def build_dimension_member_combos(dim_members, delim):
    # find all combos of dimension members in dict(dim_members), return list of member ids/names separates by delimiter
    # Examples (with dimensions numbered 1-3) should result in 1x2x2=4 possible member combinations:
    # {1: ['A1'], 2: ['B1', 'B2'], 3: ['C1', 'C2']} (for member names) = [A1-B1-C1, A1-B1-C2, A1-B2-C1, A1-B2-C2]
    # {1: [10], 2: [20, 30], 3: [40, 50]} (for member ids) = [10-20-40, 10-20-50, 10-30-40, 10-30-50]
    member_combinations = list(it.product(*(dim_members[mem] for mem in dim_members)))  # build all combos to a list
    mem_list = []
    for member_tup in member_combinations:
        mem_list.append(delim.join(map(str, member_tup)))  # turn into list of strings w/ "-" separator
    return mem_list


def build_dimension_ul(ref_year, indicator_name):
    # build custom unordered list (html) based on provided reference year and indicator name columns
    dim_ul = "<ul><li>" + ref_year + "<li>" + indicator_name.str.replace(" _ ", "<li>") + "</li></ul>"
    return dim_ul


def build_dimension_values_df(fdf, dbdf, dim_id, next_dim_val_id, next_dim_val_order):
    file_ref_dates_df = fdf
    existing_ref_dates_df = dbdf

    # join ref_dates from file to those found in the DB (ensure join column is trimmed string)
    file_ref_dates_df["REF_DATE"] = file_ref_dates_df["REF_DATE"].astype("string").str.strip()
    existing_ref_dates_df["Display_EN"] = existing_ref_dates_df["Display_EN"].astype("string").str.strip()
    joined_ref_dates_df = pd.merge(file_ref_dates_df, existing_ref_dates_df, left_on="REF_DATE",
                                   right_on="Display_EN", how="left")
    new_ref_dates_df = joined_ref_dates_df[joined_ref_dates_df['DimensionId'].isnull()].copy()  # keeps only new dates

    ret_df = pd.DataFrame()
    if new_ref_dates_df.shape[0] > 0:
        # if there are new reference dates, build remaining columns
        new_ref_dates_df["DimensionValueId"] = h.create_id_series(new_ref_dates_df, next_dim_val_id)
        new_ref_dates_df["DimensionId"] = dim_id
        new_ref_dates_df["Display_EN"] = new_ref_dates_df["REF_DATE"]  # duplicate date to FR
        new_ref_dates_df["Display_FR"] = new_ref_dates_df["Display_EN"]  # duplicate date to FR
        new_ref_dates_df["ValueDisplayOrder"] = h.create_id_series(new_ref_dates_df, next_dim_val_order)

        # order columns for db insert
        ret_df = new_ref_dates_df.loc[:, ["DimensionValueId", "DimensionId", "Display_EN", "Display_FR",
                                          "ValueDisplayOrder"]]

    return ret_df


def build_geographic_level_for_indicator_df(gldf, idf):
    # build the data frame for GeographicLevelForIndicator based on dataframe geographic levels abnd indicator codes
    # (gldf) and df of Indicator codes and Ids that were just inserted to the db (idf).
    df_gli = gldf
    pattern = "|".join(["S0504", "S0505", "S0506"])  # S0504(CA),S0505(CMAP),S0506(CAP)->S0503(CMA) (from orig PowerBI)
    df_gli["GeographicLevelId"] = df_gli["GeographicLevelId"].str.replace(pattern, "S0503")
    df_gli.drop_duplicates(inplace=True)  # remove any dupe rows

    df_gli = pd.merge(df_gli, idf, on="IndicatorCode", how="left")  # join datasets
    df_gli.drop(["IndicatorCode"], axis=1, inplace=True)  # no longer need col
    df_gli.dropna(inplace=True)  # remove any row w/ na
    df_gli = df_gli.loc[(df_gli.GeographicLevelId != "")]  # remove any row w/ empty geolevel

    # every IndicatorID needs a row added with GeographicLevel = "SSSS" (for web display)
    df_web_inds = df_gli.loc[:, ["IndicatorId"]].drop_duplicates(inplace=False)
    df_web_inds["GeographicLevelId"] = "SSSS"
    df_gli = df_gli.append(df_web_inds)
    del df_web_inds

    df_gli = df_gli.loc[:, ["IndicatorId", "GeographicLevelId"]]  # column order needeed for db insert
    return df_gli


def build_geography_reference_for_indicator_df(edf, idf, gdf, ivdf):
    # Build the data frame for GeographicReferenceForIndicator based on dataframe of english csv file (edf),
    # GeographyReference ids (gdf), Indicator # codes and Ids that were just inserted to the db (idf), and
    # Indicator Values that were just added to the db (ivdf).
    df_gri = edf.loc[:, ["DGUID", "IndicatorCode", "ReferencePeriod"]]  # subset of full en dataset
    df_gri = pd.merge(df_gri, idf, on="IndicatorCode", how="left")  # join datasets
    df_gri["IndicatorValueCode"] = df_gri["DGUID"] + "." + df_gri["IndicatorCode"]  # combine DGUID and IndicatorCode

    df_gri = pd.merge(df_gri, gdf, left_on="DGUID", right_on="GeographyReferenceId", how="left")  # join geoRef for id
    df_null_geo_rf = check_null_geography_reference(df_gri)  # notify user of any DGUIDs w/o matching geoRef

    df_gri.dropna(subset=["GeographyReferenceId", "DGUID"], inplace=True)  # drop rows with empty ids
    df_gri.drop(["GeographyReferenceId"], axis=1, inplace=True)  # drop ref column used for merge
    df_gri.rename(columns={"DGUID": "GeographyReferenceId"}, inplace=True)  # rename to match db

    df_gri = pd.merge(df_gri, ivdf, on="IndicatorValueCode", how="left")  # join to IndicatorValues for id
    df_gri.drop(["IndicatorCode", "IndicatorValueCode"], axis=1, inplace=True)
    df_gri.dropna(inplace=True)  # remove any rows w/ empty values

    # Ensure columns are in order needed for insert, convert types as required
    df_gri = df_gri.loc[:, ["GeographyReferenceId", "IndicatorId", "IndicatorValueId", "ReferencePeriod"]]
    df_gri["GeographyReferenceId"] = df_gri["GeographyReferenceId"].astype("string").str[:25]
    df_gri["ReferencePeriod"] = df_gri["ReferencePeriod"].astype("datetime64[ns]")

    return df_gri, df_null_geo_rf


def build_indicator_code(coordinate, reference_date, pid_str):
    # builds custom indicator code that strips geography from the coordinate and adds a reference date
    temp_coordinate = coordinate.str.replace(r"^([^.]+\.)", "", regex=True)  # strips 1st dimension (geography)
    indicator_code = pid_str + "." + temp_coordinate + "." + reference_date + "-01-01"  # ex. 13100778.1.23.1.2018-01-01
    return indicator_code


def build_indicator_df(product_id, release_dt, dim_members, uom_codeset, ref_date_list, next_id):

    df = create_dimension_member_df(dim_members)  # turn dimension/member data into dataframe
    df.sort_values(by=["DimPosId", "MemberId"], inplace=True)  # Important to allow recombining columns in df later

    # prepare dictionaries for creating member combinations
    dim_mem_ids = {}  # for coordinates
    dim_mem_names_en = {}  # for english indicator name
    dim_mem_names_fr = {}  # for french indicator name
    dim_mem_uoms = {}  # for unit of measure (will only occur one per member)

    for index, row in df.iterrows():
        dim_id = row["DimPosId"]

        # skip dimension 1 (geography)
        if row["DimNameEn"] != "Geography":
            if dim_id not in dim_mem_names_en:
                dim_mem_names_en[dim_id] = []
            if dim_id not in dim_mem_names_fr:
                dim_mem_names_fr[dim_id] = []
            if dim_id not in dim_mem_ids:
                dim_mem_ids[dim_id] = []
            if dim_id not in dim_mem_uoms:
                dim_mem_uoms[dim_id] = []
            dim_mem_names_en[dim_id].append(row["MemberNameEn"])
            dim_mem_names_fr[dim_id].append(row["MemberNameFr"])
            dim_mem_ids[dim_id].append(row["MemberId"])
            app_uom = str(row["MemberUomCode"]) if row["DimHasUom"] else ""  # keeps "nan" from ending up in the combo
            dim_mem_uoms[dim_id].append(app_uom)

    # build all possible member combinations
    mem_names_en = build_dimension_member_combos(dim_mem_names_en, " _ ")
    mem_names_fr = build_dimension_member_combos(dim_mem_names_fr, " _ ")
    mem_ids = build_dimension_member_combos(dim_mem_ids, ".")
    mem_uoms = build_dimension_member_combos(dim_mem_uoms, " ")

    pre_df = False
    # because the dicts are already sorted we can safely stick them together as columns in a dataframe at the end.
    if len(mem_names_en) == len(mem_names_fr) == len(mem_ids) == len(mem_uoms):
        pre_df = pd.DataFrame({"IndicatorName_EN": mem_names_en, "IndicatorName_FR": mem_names_fr,
                               "Coordinate": mem_ids, "UOM_ID": mem_uoms}, dtype=str)

    # UOM - Combining members may result in the uom field looking like "nan nan 229.0", we only want the 229 part.
    # Must go to float before int to prevent conversion error
    pre_df["UOM_ID"] = pre_df["UOM_ID"].str.replace("nan", "").str.replace(" ", "").astype("float").astype("int16")
    # Turn off inspection next 2 lines, false-positives from pycharm: see https://youtrack.jetbrains.com/issue/PY-43841
    # noinspection PyTypeChecker
    pre_df["UOM_EN"] = pre_df.apply(lambda x: h.get_uom_desc_from_code_set(x["UOM_ID"], uom_codeset, "en"), axis=1)
    # noinspection PyTypeChecker
    pre_df["UOM_FR"] = pre_df.apply(lambda x: h.get_uom_desc_from_code_set(x["UOM_ID"], uom_codeset, "fr"), axis=1)

    pre_df["IndicatorThemeID"] = product_id
    pre_df["ReleaseIndicatorDate"] = release_dt
    pre_df["Vector"] = np.NaN  # Vector field exists in gis.Indicator but is not used. We will insert nulls.
    pre_df["IndicatorNameLong_EN"] = pre_df["IndicatorName_EN"]  # just a copy of a field required for db
    pre_df["IndicatorNameLong_FR"] = pre_df["IndicatorName_FR"]

    # Create new indicator data frame with a row for each year in the reference period
    rd_years = h.get_years_range(ref_date_list)
    ind_df = copy_data_frames_for_year_range(pre_df, rd_years)

    # add the remaining fields that required RefYear to be built first
    ind_df["RefYear"] = ind_df["RefYear"].astype("string")
    ind_df["ReferencePeriod"] = ind_df["RefYear"] + "-01-01"  # becomes Jan 1
    ind_df["IndicatorCode"] = str(product_id) + "." + ind_df["Coordinate"] + "." + ind_df["ReferencePeriod"]
    ind_df["IndicatorDisplay_EN"] = build_dimension_ul(ind_df["RefYear"], ind_df["IndicatorName_EN"])
    ind_df["IndicatorDisplay_FR"] = build_dimension_ul(ind_df["RefYear"], ind_df["IndicatorName_FR"])
    ind_df["IndicatorId"] = h.create_id_series(ind_df, next_id)  # populate IDs
    # build fields needed later for IndicatorMetaData DimensionUniqueKey matching and RelatedCharts
    ind_df["IndicatorFmt"] = ind_df["RefYear"] + "-" + ind_df["IndicatorName_EN"].str.replace(" _ ", "-")
    ind_df["LastIndicatorMember_EN"] = ind_df.apply(lambda x: get_last_member_from_indicator_name(
        x["IndicatorName_EN"]), axis=1)
    ind_df["LastIndicatorMember_FR"] = ind_df.apply(lambda x: get_last_member_from_indicator_name(
        x["IndicatorName_FR"]), axis=1)

    # set datatypes for db
    ind_df["ReleaseIndicatorDate"] = ind_df["ReleaseIndicatorDate"].astype("datetime64[ns]")
    ind_df["ReferencePeriod"] = ind_df["ReferencePeriod"].astype("datetime64[ns]")
    ind_df["IndicatorCode"] = ind_df["IndicatorCode"].str[:100]
    return ind_df


def build_indicator_df_subset(idf):
    # for the indicator dataframe (idf), return only those rows needed for database inserts
    df = idf.loc[:, ["IndicatorId", "IndicatorName_EN", "IndicatorName_FR", "IndicatorThemeID", "ReleaseIndicatorDate",
                     "ReferencePeriod", "IndicatorCode", "IndicatorDisplay_EN", "IndicatorDisplay_FR", "UOM_EN",
                     "UOM_FR", "Vector", "IndicatorNameLong_EN", "IndicatorNameLong_FR"]]
    return df


def build_indicator_metadata_df(idf, prod_defaults, dkdf, existing_md_df):
    # Build the data frame for IndicatorMetadata using the indicator dataset (idf), product defaults (prod_defaults)
    # and unique dimension keys (dkdf). If the metadata for an indicator already exists (existing_meta_data) use it,
    # otherwise use the product default (prod_defaults).

    # formatted indicator names in idf can merged with unique dimension keys data frame
    idf["IndicatorFmt_Lower"] = idf["IndicatorFmt"].str.lower()  # prevents case sensitivity issues during merge
    dkdf["IndicatorFmt_Lower"] = dkdf["IndicatorFmt"].str.lower()
    idf = pd.merge(idf, dkdf, on="IndicatorFmt_Lower", how="left")
    check_null_dimension_unique_keys(idf, True)  # notify user of any missing unique dimension keys
    ind_subset_df = idf.loc[:, ["IndicatorId", "UOM_EN", "UOM_FR", "UOM_ID", "DimensionUniqueKey", "IndicatorCode"]]

    # merge with the df of existing metadata (subsetted).
    sub_existing_md_df = existing_md_df.loc[:, ["IndicatorCode", "DefaultBreaksAlgorithmId", "DefaultBreaks",
                                                "PrimaryChartTypeId", "ColorTo", "ColorFrom"]]
    df_im = pd.merge(ind_subset_df, sub_existing_md_df, on="IndicatorCode", how="left")
    df_im.drop_duplicates(subset="IndicatorId", keep="first", inplace=True)

    # gis.Indicator columns that can be reused for gis.IndicatorMetaData
    df_im["MetaDataId"] = df_im["IndicatorId"]  # duplicate column
    df_im["DefaultRelatedChartId"] = df_im["IndicatorId"]  # duplicate column
    df_im.rename(columns={"UOM_EN": "FieldAlias_EN", "UOM_FR": "FieldAlias_FR", "UOM_ID": "DataFormatId"}, inplace=True)

    # set default metadata for product if no existing metadata exists (value will be None if it does not exist)
    df_im["DefaultBreaksAlgorithmId"].fillna(prod_defaults["default_breaks_algorithm_id"], inplace=True)
    df_im["DefaultBreaks"].fillna(prod_defaults["default_breaks"], inplace=True)
    df_im["PrimaryChartTypeId"].fillna(prod_defaults["primary_chart_type_id"], inplace=True)
    df_im["ColorTo"].fillna(prod_defaults["color_to"], inplace=True)
    df_im["ColorFrom"].fillna(prod_defaults["color_from"], inplace=True)

    # uom formats are inserted into the primary query
    df_im["en_format"] = df_im.apply(lambda x: set_uom_format(x["DataFormatId"], "en", x["PrimaryChartTypeId"]), axis=1)
    df_im["fr_format"] = df_im.apply(lambda x: set_uom_format(x["DataFormatId"], "fr", x["PrimaryChartTypeId"]), axis=1)

    df_im["PrimaryQuery"] = "SELECT iv.value AS Value, CASE WHEN iv.value IS NULL THEN nr.symbol ELSE " + \
                            df_im["en_format"] + " END AS FormattedValue_EN,  CASE WHEN iv.value IS NULL THEN " \
                            "nr.symbol ELSE " + df_im["fr_format"] + " END AS FormattedValue_FR, " \
                            "grfi.GeographyReferenceId, g.DisplayNameShort_EN, g.DisplayNameShort_FR, " \
                            "g.DisplayNameLong_EN, g.DisplayNameLong_FR, g.ProvTerrName_EN, g.ProvTerrName_FR, " \
                            "g.Shape, i.IndicatorName_EN, i.IndicatorName_FR, i.IndicatorId, i.IndicatorDisplay_EN, " \
                            "i.IndicatorDisplay_FR, i.UOM_EN, i.UOM_FR, g.GeographicLevelId, gl.LevelName_EN, " \
                            "gl.LevelName_FR, gl.LevelDescription_EN, gl.LevelDescription_FR, g.EntityName_EN, " \
                            "g.EntityName_FR, nr.Symbol, nr.Description_EN as NullDescription_EN, nr.Description_FR " \
                            "as NullDescription_FR FROM gis.geographyreference AS g INNER JOIN " \
                            "gis.geographyreferenceforindicator AS grfi ON g.geographyreferenceid = " \
                            "grfi.geographyreferenceid  INNER JOIN (select * from gis.indicator where " \
                            "indicatorId = " + df_im["IndicatorId"].astype(str) + ") AS i ON grfi.indicatorid = " \
                            "i.indicatorid  INNER JOIN gis.geographiclevel AS gl ON g.geographiclevelid = " \
                            "gl.geographiclevelid  INNER JOIN gis.geographiclevelforindicator AS glfi  ON " \
                            "i.indicatorid = glfi.indicatorid  AND gl.geographiclevelid = glfi.geographiclevelid " \
                            "INNER JOIN gis.indicatorvalues AS iv  ON iv.indicatorvalueid = grfi.indicatorvalueid  " \
                            "INNER JOIN gis.indicatortheme AS it ON i.indicatorthemeid = it.indicatorthemeid  " \
                            "LEFT OUTER JOIN gis.indicatornullreason AS nr ON iv.nullreasonid = nr.nullreasonid"

    # set datatypes/lengths for db
    df_im["FieldAlias_EN"] = df_im["FieldAlias_EN"].astype("string").str[:600]
    df_im["FieldAlias_FR"] = df_im["FieldAlias_FR"].astype("string").str[:600]
    df_im["DimensionUniqueKey"] = df_im["DimensionUniqueKey"].astype("string").str[:50]
    df_im["ColorTo"] = df_im["ColorTo"].astype("string").str[:35]
    df_im["ColorFrom"] = df_im["ColorFrom"].astype("string").str[:35]
    df_im["PrimaryQuery"] = df_im["PrimaryQuery"].astype("string").str[:4000]

    # Order columns for insert
    df_im = df_im.loc[:, ["MetaDataId", "IndicatorId", "FieldAlias_EN", "FieldAlias_FR", "DataFormatId",
                          "DefaultBreaksAlgorithmId", "DefaultBreaks", "PrimaryChartTypeId", "PrimaryQuery",
                          "ColorTo", "ColorFrom", "DimensionUniqueKey", "DefaultRelatedChartId"]]
    return df_im


def build_indicator_values_df(edf, gdf, ndf, next_id):
    # build the data frame for IndicatorValues
    # based on dataframe of english csv file (edf), GeographyReference ids (gdf), and NullReason ids (ndf).
    # populate indicator value ids starting from next_id.
    # also collect and return unique GeographicLevelIDs

    df_iv = edf.loc[:, ["DGUID", "IndicatorCode", "STATUS", "VALUE"]]  # subset of full en dataset
    df_iv["IndicatorValueId"] = h.create_id_series(edf, next_id)  # populate IDs
    df_iv = pd.merge(df_iv, gdf, left_on="DGUID", right_on="GeographyReferenceId", how="left")  # join to geoRef for id

    df_iv.dropna(subset=["GeographyReferenceId"], inplace=True)  # drop empty ids
    df_iv.drop(["GeographyReferenceId"], axis=1, inplace=True)
    df_iv["IndicatorValueCode"] = df_iv["DGUID"] + "." + df_iv["IndicatorCode"]  # combine DGUID and IndicatorCode
    df_iv.drop(["DGUID", "IndicatorCode"], axis=1, inplace=True)
    df_iv = pd.merge(df_iv, ndf, left_on="STATUS", right_on="Symbol", how="left")  # join to NullReasonId for Symbol
    df_iv.drop(["STATUS", "Symbol"], axis=1, inplace=True)

    # set datatypes for db
    df_iv = df_iv.fillna(np.nan).replace([np.nan], [None])  # workaround to set nan/na=None (prevents sql error 22003)
    df_iv["IndicatorValueCode"] = df_iv["IndicatorValueCode"].str[:100]

    # Keep only the columns needed for insert
    df_iv = df_iv.loc[:, ["IndicatorValueId", "VALUE", "NullReasonId", "IndicatorValueCode"]]
    return df_iv


def build_reference_date_list(start_str, end_str, freq_code):
    # build list of dates from start_str to end_str (will always be a string in YYYY-MM-DD format) based on freq_code
    # (code from WDS indicating how often the data is published). Returns dates as pandas series (datetime64[ns])
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")

    freq_dict = {
        1: "D",  # daily
        2: "W",  # weekly (sun)
        4: "2W",  # every 2 weeks (sun)
        6: "MS",  # monthly as start of month
        7: "2MS",  # every 2 months, interpreted as every 2 months at start of month
        9: "QS",  # quarterly as start of quarter
        10: "4MS",  # 3 times per year, interpreted as every 4 months at start of month
        11: "6MS",  # semi-annual, interpreted as every 6 months at start of month
        12: "AS",  # annual as start of year
        13: "2AS",  # every 2 years, interpreted as every 2 years at start of year
        14: "3AS",  # every 3 years, interpreted as every 3 years at start of year
        15: "4AS",  # every 4 years, interpreted as every 4 years at start of year
        16: "5AS",  # every 5 years, interpreted as every 5 years at start of year
        17: "10AS",  # every 10 years, interpreted as every 10 years at start of year
        18: "AS",  # occasional annual as start of year
        19: "QS",  # occasional quarterly as start of quarter
        20: "MS",  # occasional monthly as start of month
        21: "D"  # occasional daily as daily
    }
    fr = freq_dict[freq_code] if freq_code in freq_dict else "AS"  # default to first of each year if not in dict
    retval = pd.date_range(start_dt, end_dt, freq=fr)
    return retval


def build_related_charts_df(idf, prod_defaults, existing_md_df):
    # Build the data frame for RelatedCharts using the indicator dataset (idf). If the metadata for an indicator
    # already exists (existing_meta_data) use it, otherwise use the product default (prod_defaults).

    ind_subset_df = idf.loc[:, ["IndicatorId", "IndicatorCode", "UOM_ID", "LastIndicatorMember_EN",
                                "LastIndicatorMember_FR", "UOM_EN", "UOM_FR"]]  # subset to needed fields
    sub_existing_md_df = existing_md_df.loc[:, ["IndicatorCode", "ChartTypeId", "ChartTitle_EN", "ChartTitle_FR",
                                                "FieldAlias_EN", "FieldAlias_FR"]]  # subset to needed fields
    df_rc = pd.merge(ind_subset_df, sub_existing_md_df, on="IndicatorCode", how="left")  # merge with existing metadata

    # gis.Indicator columns that can be reused for gis.RelatedCharts
    df_rc["RelatedChartId"] = df_rc["IndicatorId"]  # duplicate column
    df_rc["IndicatorMetaDataId"] = df_rc["IndicatorId"]  # duplicate column
    df_rc.rename(columns={"UOM_ID": "DataFormatId"}, inplace=True)

    # set defaults for product if no existing metadata exists (value will be None if it does not exist)
    df_rc["ChartTypeId"].fillna(prod_defaults["related_chart_type_id"], inplace=True)
    df_rc["ChartTitle_EN"].fillna(df_rc["LastIndicatorMember_EN"], inplace=True)
    df_rc["ChartTitle_FR"].fillna(df_rc["LastIndicatorMember_FR"], inplace=True)
    df_rc["FieldAlias_EN"].fillna(df_rc["UOM_EN"], inplace=True)
    df_rc["FieldAlias_FR"].fillna(df_rc["UOM_FR"], inplace=True)

    # uom formats are inserted into the query field
    df_rc["en_format"] = df_rc.apply(lambda x: set_uom_format(x["DataFormatId"], "en", x["ChartTypeId"]), axis=1)
    df_rc["fr_format"] = df_rc.apply(lambda x: set_uom_format(x["DataFormatId"], "fr", x["ChartTypeId"]), axis=1)

    # find default related indicators by building a generic indicator code and then finding all matching ids
    # these indicator ids will get added to the query that gets saved in the db.
    df_rc["GenericIndicatorCode"] = df_rc.apply(lambda x: set_generic_indicator_code(x["IndicatorCode"]), axis=1)
    df_rc["RelatedIndicatorIDs"] = df_rc.apply(lambda x: get_related_indicator_list(x["GenericIndicatorCode"],
                                                                                    x["IndicatorId"], df_rc), axis=1)

    df_rc["Query"] = "SELECT iv.value AS Value, CASE WHEN iv.value IS NULL THEN nr.symbol ELSE " + df_rc["en_format"] \
                     + " END AS FormattedValue_EN, CASE WHEN iv.value IS NULL THEN nr.symbol ELSE " + \
                     df_rc["fr_format"] + " END AS FormattedValue_FR, i.IndicatorName_EN, i.IndicatorName_FR, " \
                     "nr.Description_EN AS NullDescription_EN, nr.Description_FR AS NullDescription_FR FROM " \
                     "gis.IndicatorValues AS iv left outer join gis.IndicatorNullReason AS nr on iv.NullReasonId = " \
                     "nr.NullReasonId INNER JOIN gis.GeographyReferenceForIndicator AS gfri ON iv.indicatorvalueid = " \
                     "gfri.indicatorvalueid INNER JOIN gis.indicator AS i ON i.indicatorid = gfri.indicatorid WHERE " \
                     "gfri.indicatorid IN (" + df_rc["RelatedIndicatorIDs"] + ")"

    # set datatypes/lengths for db
    df_rc["ChartTitle_EN"] = df_rc["ChartTitle_EN"].astype("string").str[:150]
    df_rc["ChartTitle_FR"] = df_rc["ChartTitle_FR"].astype("string").str[:150]
    df_rc["Query"] = df_rc["Query"].astype("string").str[:4000]
    df_rc["FieldAlias_EN"] = df_rc["FieldAlias_EN"].astype("string").str[:150]
    df_rc["FieldAlias_FR"] = df_rc["FieldAlias_FR"].astype("string").str[:150]

    # Order columns for insert
    df_rc = df_rc.loc[:, ["RelatedChartId", "ChartTitle_EN", "ChartTitle_FR", "Query", "ChartTypeId",
                          "IndicatorMetaDataId", "DataFormatId", "FieldAlias_EN", "FieldAlias_FR"]]
    return df_rc


def check_null_dimension_unique_keys(df, show_warnings):
    # notify user if there are any missing DimensionUniqueKeys in the df and show_warnings is true
    missing_keys_df = df[df["DimensionUniqueKey"].isnull()]
    if show_warnings and missing_keys_df.shape[0] > 0:
        log.warning("\n***WARNING***\nDimensionUniqueKey could not be matched for the following indicators:")
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            missing_keys_df = missing_keys_df.loc[:, ["IndicatorId", "IndicatorCode"]]
            log.warning(missing_keys_df)
        log.warning("*************\n")
    return


def check_null_geography_reference(df):
    # alert user w/ DGUID if any "GeographyReferenceId" in df has nulls
    df_null_gr = df[df["GeographyReferenceId"].isna()].loc[:, ["DGUID"]].drop_duplicates(inplace=False)
    return df_null_gr


def copy_data_frames_for_year_range(df_to_copy, year_list):
    # When passed a dataframe (df_to_copy) and a list of years(year_list), build a copy of the dataframe
    # for each year and add it to a list. The list is then combined into one big dataframe and returned in ref_df.
    df_list = []
    for rel_year in year_list:
        tmp_df = df_to_copy.copy()
        tmp_df["RefYear"] = rel_year
        df_list.append(tmp_df)
    ret_df = pd.concat(df_list)  # combine into one dataframe
    return ret_df


def create_dimension_member_df(dim_members):
    # from dimension/member json --> # build data frame of dimension and member info, return as df
    rows_list = []
    for dim in dim_members:
        for mem in dim["member"]:
            dim_dict = {"DimPosId": dim["dimensionPositionId"], "DimNameEn": dim["dimensionNameEn"],
                        "DimNameFr": dim["dimensionNameFr"], "DimHasUom": dim["hasUom"], "MemberId": mem["memberId"],
                        "MemberNameEn": mem["memberNameEn"], "MemberNameFr": mem["memberNameFr"],
                        "MemberUomCode": mem["memberUomCode"]}
            rows_list.append(dim_dict)

    dm_df = pd.DataFrame(rows_list)
    return dm_df


def get_last_member_from_indicator_name(indicator_name):
    # for the given indicator name, pop off and return the last member
    # Example: "Property with multiple residential units _ Vacant land _ Number of owners"
    # Becomes: "Number of owners"
    split_ind = indicator_name.split(" _ ")  # separator between dimensions/members
    retval = split_ind[-1]
    return retval


def fix_dguid(vintage, orig_dguid, is_crime_table):
    # verify that the dguid is valid. If it isn't, try to build a new one. For now this only applies to tables
    # flagged as a crime table. Format: VVVVTSSSSGGGGGGGGGGGG (V-vintage(4), T-type(1), S-schema(4), G-GUID(1-12)
    new_dguid = str(orig_dguid)
    if is_crime_table and len(orig_dguid) < 10:
        geo_type_schema = 'A0025'
        if int(vintage) < 2016:  # special case from crime tables --> 1998-2015 use 2016 geographies.
            new_dguid = '2016' + geo_type_schema + orig_dguid
        else:
            new_dguid = str(vintage) + geo_type_schema + orig_dguid
    return new_dguid


def get_related_indicator_list(generic_ind_code, ind_id, idf):
    # For the specified indicator dataframe (idf), find rows with an IndicatorCode that matches generic_ind_code.
    # Build a list of the matching IndicatorIDs (exclude the current id), and return the list as a comma
    # separated string. This is used to find related indicator ids for gis.RelatedCharts.
    filtered_ind_df = idf.loc[(idf["GenericIndicatorCode"] == generic_ind_code) & (idf["IndicatorId"] != ind_id)]
    filtered_ind_df["IndicatorId"] = filtered_ind_df["IndicatorId"].astype("string")
    indicator_id_list = filtered_ind_df["IndicatorId"].tolist()
    indicator_id_str = ','.join(indicator_id_list)
    return indicator_id_str


def set_generic_indicator_code(ind_code):
    # Take the indicator code (ind_code), and return a more generic version with the second to last element in
    # the coordinate replaced by a wildcard character
    # ex. "13100778.4.1.2.1.2018-01-01" becomes "13100778.4.1.%.1.2018-01-01"
    split_ind_code = ind_code.split(".")  # returns list of all elements in the indicator code
    generic_ind_code = None
    if len(split_ind_code) > 3:  # check for valid code length (at least 2 dimensions in coordinate part of code
        generic_ind_code = ".".join(split_ind_code[0:len(split_ind_code) - 3]) \
                           + ".%." + ".".join(split_ind_code[-2:])  # stitch code back together with wildcard inserted
    return generic_ind_code


def set_uom_format(uom_id, lang, chart_type_id):
    # Returns format string for specified uom_id, language (lang), chart_type_id (1-bar, 2-pie, 3-line)
    # These formats were selected based on what already existed in the database as built by the powerBI process.
    loc_code = "en-US"
    if lang == "fr":
        loc_code = "fr-CA"

    # default
    format_str = "Format(iv.value, 'N', '" + loc_code + "')"
    if uom_id == 223 or uom_id == 249 or (uom_id == 239 and chart_type_id == 1):
        format_str = "Format(iv.value, 'N0', '" + loc_code + "')"
    elif uom_id == 81:
        format_str = "Format(iv.value, 'C0', '" + loc_code + "')"
    elif (uom_id == 239 and chart_type_id == 2) or (uom_id == 279):
        format_str = "Format(iv.value/100, 'P1', '" + loc_code + "')"
    return format_str


def setup_chunk_columns(cdf, prod_id_str, rel_date, is_crime_table):
    # set up the columns in a dataframe chunk of data from the csv file (cdf) for the specified product (prod_id_str)
    # and release date (rel_date). is_crime_table indicates whether it is one of the crime tables.
    chunk_df = cdf
    chunk_df["IndicatorCode"] = build_indicator_code(chunk_df["COORDINATE"], chunk_df["REF_DATE"], prod_id_str)
    chunk_df.drop(["COORDINATE"], axis=1, inplace=True)  # not nec. after IndicatorCode
    chunk_df.rename(columns={"VECTOR": "Vector", "UOM": "UOM_EN"}, inplace=True)  # match db
    chunk_df["RefYear"] = chunk_df["REF_DATE"].map(h.fix_ref_year).astype("string")  # need 4 digit year

    chunk_df["DGUID"] = chunk_df["DGUID"].str.replace(".", "").str.replace("201A", "2015A")  # from powerBI process
    # some DGUIDs are not built properly in the dataset (ex. crime statistics) - fix these
    chunk_df["DGUID"] = chunk_df.apply(lambda x: fix_dguid(x["RefYear"], x["DGUID"], is_crime_table), axis=1)

    chunk_df["IndicatorThemeID"] = prod_id_str
    chunk_df["ReleaseIndicatorDate"] = rel_date
    chunk_df["ReferencePeriod"] = chunk_df["RefYear"] + "-01-01"  # becomes Jan 1
    chunk_df["ReferencePeriod"] = chunk_df["ReferencePeriod"].astype("datetime64[ns]")
    chunk_df["Vector"] = chunk_df["Vector"].str.replace("v", "").astype("int32")
    chunk_df["GeographicLevelId"] = chunk_df["DGUID"].str[4:9]  # extract geo level id
    return chunk_df


def write_dguid_warning(dguid_df):
    # turn a dataframe of dguids (dguid_df) into a warning that can be written to a log file or console.
    dguid_df.dropna(inplace=True)
    dguid_df.drop_duplicates(inplace=True)
    if dguid_df.shape[0] > 0:
        msg = "***WARNING***\nThe following DGUIDs were not found in gis.GeographyReference and cannot be added to " \
              "the database.\nAny values other than <NA>/NaN should be investigated.\n"
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            msg += dguid_df["DGUID"].to_string(index=False)  # if the DGUID is <NA>, then there is no problem
        msg += "\n*************\n"
    else:
        msg = "All expected DGUIDs were found in gis.GeographyReference."
    return msg
