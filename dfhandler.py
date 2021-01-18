# data frame handling

import helpers as h  # helper functions
import itertools as it  # for iterators
import numpy as np
import pandas as pd
import re  # regular expressions


def build_column_and_type_dict(dimensions, lang):
    # set up the dicionary of columns and data types for pandas df
    # add columns listed in dimensions to the predefined columns below.
    # returns dictionary of cols:types for en and fr
    # Note:
    #   df returns all strings as object type by default
    #   category more efficient for string fields if there are < 50% unique values and no string operations are required
    if lang == "en":
        cols = {"REF_DATE": "string", "DGUID": "string", "UOM": "category", "UOM_ID": "int16", "VECTOR": "string",
                "COORDINATE": "string", "STATUS": "category", "SYMBOL": "string", "VALUE": "float64"}
    else:
        cols = {"PÉRIODE DE RÉFÉRENCE": "string", "COORDONNÉES": "string", "UNITÉ DE MESURE": "category"}

    for dim in dimensions[lang]:
        cols[dim] = "string"
    return cols


def build_dimension_unique_keys(dmf):
    # Build a dataframe of unique dimension keys for the product id (pid) from a dataset returned from
    # gis.Dimensions and gis.DimensionValues (dmf).
    # The unique keys are the ordered and concatenated index values of each member in gis.DimensionValues.
    # There are no IndicatorIds, vectors, or coordinates in these tables, so we are figuring out the link to Indicator
    # backward through reference periods and indicator names.
    dim_mem_names = {}
    dim_mem_ids = {}

    # build dictionaries of member ids and member names
    for index, row in dmf.iterrows():
        dim_id = row["DimensionId"]
        mem_id = row["DimensionValueId"]
        # strips leading number/period/whitespace from member name (ex. "02. Resident owners only" removes "02. ")
        mem_name = re.sub(r"^([^.]+\.)", "", row["Display_EN"]).lstrip()

        if dim_id not in dim_mem_names:
            dim_mem_names[dim_id] = []
        dim_mem_names[dim_id].append(mem_name)

        if dim_id not in dim_mem_ids:
            dim_mem_ids[dim_id] = []
        dim_mem_ids[dim_id].append(mem_id)

    mem_names = build_dimension_member_combos(dim_mem_names)
    mem_ids = build_dimension_member_combos(dim_mem_ids)
    keys_df = False
    if len(mem_names) == len(mem_ids):
        # can combine the two lists b/c they are in the same order by dimension
        keys_df = pd.DataFrame({"IndicatorFmt": mem_names, "DimensionUniqueKey": mem_ids})

    return keys_df


def build_dimension_member_combos(dim_members):
    # find all possible combinations of dimension members in dictionary(dim_members)
    # dictionary examples (with dimensions numbered 1-3):
    # {1: ['A1'], 2: ['B1', 'B2'], 3: ['C1', 'C2']} (for member names)
    # {1: [10], 2: [20, 30], 3: [40, 50]} (for member ids)
    # returns list member ids or member names separates by "-"
    # ex. if dimensions A-->C exist with 2 members each. This should result in 1x2x2=4 possible combinations
    #   A1-B1-C1 10-20-40
    #   A1-B1-C2 10-20-50
    #   A1-B2-C1 10-30-40
    #   A1-B2-C2 10-30-50

    member_combinations = list(it.product(*(dim_members[mem] for mem in dim_members)))  # build all combos to a list
    mem_list = []
    for member_tup in member_combinations:
        mem_list.append('-'.join(map(str, member_tup)))  # turn into list of strings w/ "-" separator

    return mem_list


def build_dimension_ul(ref_year, indicator_name):
    # build custom unordered list (html) based on
    # provided reference year and indicator name columns
    dim_ul = "<ul><li>" + ref_year + "<li>" + indicator_name.str.replace(" _ ", "<li>") + "</li></ul>"
    return dim_ul


def build_geographic_level_for_indicator_df(edf, idf):
    # build the data frame for GeographicLevelForIndicator
    # based on dataframe of english csv file (edf) and dataframe of Indicator
    # codes and Ids that were just inserted to the db (idf).
    print("Building GeographicLevelForIndicator table.")
    df_gli = edf.loc[:, ["DGUID", "IndicatorCode"]]  # subset of full en dataset
    df_gli["DGUID"] = df_gli["DGUID"].str[4:9]  # extract geo level id from DGUID
    df_gli.rename(columns={"DGUID": "GeographicLevelId"}, inplace=True)  # rename to match db
    pattern = "|".join(["S0504", "S0505", "S0506"])  # S0504(CA),S0505(CMAP),S0506(CAP)-->S0503(CMA)
    df_gli["GeographicLevelId"] = df_gli["GeographicLevelId"].str.replace(pattern, "S0503")
    df_gli.drop_duplicates(inplace=True)  # remove dupe rows

    df_gli = pd.merge(df_gli, idf, on="IndicatorCode", how="left")  # join datasets
    df_gli.drop(["IndicatorCode"], axis=1, inplace=True)  # no longer need col
    df_gli.dropna(inplace=True)  # remove any row w/ empty value

    # Ensure columns are in order needed for insert
    df_gli = df_gli.loc[:, ["IndicatorId", "GeographicLevelId"]]

    # every IndicatorID needs a row added with GeographicLevel = "SSSS" (for web display)
    df_web_inds = df_gli.loc[:, ["IndicatorId"]].drop_duplicates(inplace=False)
    df_web_inds["GeographicLevelId"] = "SSSS"
    df_gli = df_gli.append(df_web_inds)

    print("Finished building GeohraphicLevelForIndicator table.")
    return df_gli


def build_geography_reference_for_indicator_df(edf, idf, gdf, ivdf):
    # Build the data frame for GeographicReferenceForIndicator based on dataframe of english csv file (edf),
    # GeographyReference ids (gdf), Indicator # codes and Ids that were just inserted to the db (idf), and
    # Indicator Values that were just added to the db (ivdf).
    print("Building GeographyReferenceForIndicator table.")
    df_gri = edf.loc[:, ["DGUID", "IndicatorCode", "ReferencePeriod"]]  # subset of full en dataset
    df_gri = pd.merge(df_gri, idf, on="IndicatorCode", how="left")  # join datasets
    df_gri["IndicatorValueCode"] = df_gri["DGUID"] + "." + df_gri["IndicatorCode"]  # combine DGUID and IndicatorCode

    df_gri = pd.merge(df_gri, gdf, left_on="DGUID", right_on="GeographyReferenceId", how="left")  # join geoRef for id
    check_null_geography_reference(df_gri)  # notify user of any DGUIDs w/o matching geoRef

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

    print("Finished building GeographyReferenceForIndicator table.")
    return df_gri


def build_indicator_code(coordinate, reference_date, pid_str):
    # build a custom indicator code that strips geography from the coordinate and adds a reference date
    # IndicatorCode ex. 13100778.1.23.1.2018-01-01
    temp_coordinate = coordinate.str.replace(r"^([^.]+\.)", "", regex=True)  # strips 1st dimension (geography)
    indicator_code = pid_str + "." + temp_coordinate + "." + reference_date + "-01-01"
    return indicator_code


def build_indicator_df_start(edf, fdf):
    # edf --> english dataframe
    # fdf --> french dataframe
    # drop duplicte indicator codes from english dataframe
    # merge with french and return the merged df
    print("Building Indicator Table...")
    new_df = pd.merge(
        edf.drop_duplicates(subset=["IndicatorCode"], inplace=False),
        fdf, on="IndicatorCode"
    )
    return new_df


def build_indicator_df_end(df, dims, next_id):
    # for the given df:
    #   populate indicator ids starting from next_id
    #   concatenate dimension names to populate specified string fields
    #   fix any data types/lengths as required before db insert

    df["IndicatorId"] = create_id_series(df, next_id)  # populate IDs

    # operations on common field names for each language
    for ln in list(["EN", "FR"]):
        # concatenations
        df["IndicatorName_" + ln] = concat_dimension_cols(dims[ln.lower()], df, " _ ")
        df["IndicatorDisplay_" + ln] = build_dimension_ul(df["RefYear"], df["IndicatorName_" + ln])
        df["IndicatorNameLong_" + ln] = df["IndicatorName_" + ln]  # just a copy of a field required for db
        # field lengths and types
        df["IndicatorName_" + ln] = df["IndicatorName_" + ln].str[:1000]
        df["IndicatorDisplay_" + ln] = df["IndicatorDisplay_" + ln].str[:500]
        df["UOM_" + ln] = df["UOM_" + ln].astype("string").str[:50]
        df["IndicatorNameLong_" + ln] = df["IndicatorNameLong_" + ln].str[:1000]

    # build field needed later for IndicatorMetaData DimensionUniqueKey matching
    df["IndicatorFmt"] = df["RefYear"] + "-" + df["IndicatorName_EN"].str.replace(" _ ", "-")

    # set datatypes for db
    df["ReleaseIndicatorDate"] = df["ReleaseIndicatorDate"].astype("datetime64[ns]")
    df["IndicatorCode"] = df["IndicatorCode"].str[:100]

    print("Finished building Indicator table.")
    return df


def build_indicator_df_subset(idf):
    # for the indicator dataframe (idf), return only those rows needed for database inserts
    df = idf.loc[:, ["IndicatorId", "IndicatorName_EN", "IndicatorName_FR", "IndicatorThemeID",
                         "ReleaseIndicatorDate", "ReferencePeriod", "IndicatorCode", "IndicatorDisplay_EN",
                         "IndicatorDisplay_FR", "UOM_EN", "UOM_FR", "Vector", "IndicatorNameLong_EN",
                         "IndicatorNameLong_FR"]]
    return df


def build_indicator_metadata_df(idf, prod_defaults, dkdf):
    # build the data frame for IndicatorMetadata using the indicator dataset (idf),
    # product defaults (prod_defaults) and unique dimension keys (dkdf)

    print("Building IndicatorMetaData table.")

    # formatted indicator names in idf can merged with unique dimension keys data frame
    idf = pd.merge(idf, dkdf, on="IndicatorFmt", how="left")
    check_null_dimension_unique_keys(idf)  # notify user of any missing unique dimension keys
    df_im = idf.loc[:, ["IndicatorId", "UOM_EN", "UOM_FR", "UOM_ID", "DimensionUniqueKey"]]  # subset

    # gis.Indicator columns that can be reused for gis.IndicatorMetaData
    df_im["MetaDataId"] = df_im["IndicatorId"]  # duplicate column
    df_im["DefaultRelatedChartId"] = df_im["IndicatorId"]  # duplicate column
    df_im.rename(columns={"UOM_EN": "FieldAlias_EN", "UOM_FR": "FieldAlias_FR", "UOM_ID": "DataFormatId"},
                 inplace=True)  # rename to match db

    # set default metadata for product
    df_im["DefaultBreaksAlgorithmId"] = prod_defaults["default_breaks_algorithm_id"]
    df_im["DefaultBreaks"] = prod_defaults["default_breaks"]
    df_im["PrimaryChartTypeId"] = prod_defaults["primary_chart_type_id"]
    df_im["ColorTo"] = prod_defaults["color_to"]
    df_im["ColorFrom"] = prod_defaults["color_from"]

    # uom formats are inserted into the primary query
    df_im["en_format"] = df_im.apply(lambda x: set_uom_format(x["DataFormatId"], "en"), axis=1)
    df_im["fr_format"] = df_im.apply(lambda x: set_uom_format(x["DataFormatId"], "fr"), axis=1)

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
    print(df_im.dtypes)

    # Order columns for insert
    df_im = df_im.loc[:, ["MetaDataId", "IndicatorId", "FieldAlias_EN", "FieldAlias_FR", "DataFormatId",
                          "DefaultBreaksAlgorithmId", "DefaultBreaks", "PrimaryChartTypeId", "PrimaryQuery",
                          "ColorTo", "ColorFrom", "DimensionUniqueKey", "DefaultRelatedChartId"]]

    print("Finished building IndicatorMetaData table.")
    return df_im


def build_indicator_values_df(edf, gdf, ndf, next_id):
    # build the data frame for IndicatorValues
    # based on dataframe of english csv file (edf), GeographyReference ids (gdf), and NullReason ids (ndf).
    # populate indicator value ids starting from next_id.

    print("Building IndicatorValues table.")
    df_iv = edf.loc[:, ["DGUID", "IndicatorCode", "STATUS", "VALUE"]]  # subset of full en dataset
    df_iv["IndicatorValueId"] = create_id_series(edf, next_id)  # populate IDs
    df_iv = pd.merge(df_iv, gdf, left_on="DGUID", right_on="GeographyReferenceId", how="left")  # join to geoRef for id
    check_null_geography_reference(df_iv)  # notify user of any DGUIDs w/o matching geoRef

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

    print("Finished building IndicatorValues table.")
    return df_iv


def check_null_dimension_unique_keys(df):
    # notify user if there are any missing DimensionUniqueKeys in the df
    missing_keys_df = df[df["DimensionUniqueKey"].isnull()]
    if missing_keys_df.shape[0] > 0:
        print("***WARNING***\nDimensionUniqueKey could not be matched for the following indicators:")
        with pd.option_context('display.max_rows', None):
            print(missing_keys_df)
    print("*************")


def check_null_geography_reference(df):
    # alert user w/ DGUID if any "GeographyReferenceId" in df has nulls
    df_null_gr = df[df["GeographyReferenceId"].isna()].loc[:, ["DGUID"]].drop_duplicates(inplace=False)
    if df_null_gr.shape[0] > 0:
        print("***WARNING***\nThe following DGUIDs were not found in gis.GeographyReference and cannot be added "
              "to the database.\nAny values other than <NA> should be investigated.")
        with pd.option_context('display.max_rows', None):
            print(df_null_gr["DGUID"])  # if the DGUID is also <NA>, then there is no problem
        print("*************")
    return


def concat_dimension_cols(dimensions, df, delimiter):
    # concatenate data frame columns by dimension name
    #   dimensions = list of dimensions
    #   df = data frame containing columns with names that match dimension list
    #   delimiter = text to insert between dimension values
    # returns a column (pandas series) with all dimension values joined
    retval = ""
    first_col = True

    for dimension in dimensions:
        if first_col:
            retval = df[dimension]
            first_col = False
        else:
            retval += delimiter + df[dimension]

    return retval


def convert_csv_to_df(csv_file_name, delim, cols):
    # read specified csv in chunks
    # cols = dict of columns and data types colname:coltype
    # return as pandas dataframe
    prod_rows = []
    print("Reading file to dataframe: " + csv_file_name + "\n")

    for chunk in pd.read_csv(csv_file_name, chunksize=10000, sep=delim, usecols=list(cols.keys()), dtype=cols):
        prod_rows.append(chunk)
    csv_df = pd.concat(prod_rows)

    return csv_df


def create_id_series(df, start_id):
    # returns a series of ids for a dataframe starting from start_id
    return pd.RangeIndex(start=start_id, stop=(start_id + df.shape[0]))


def load_and_prep_prod_df(csvfile, dims, language, delim, prod_id, rel_date):
    # load a product csv file and prepare it for further processing
    #   csvfile --> path to an unzipped csv
    #   dims --> dimensions
    #   lang --> language en or fr
    #   prod_id  --> product id
    #   rel_date --> release_date
    # returns a dataframe

    col_dict = build_column_and_type_dict(dims, language)  # columns and data types dict
    df = convert_csv_to_df(csvfile, delim, col_dict)  # load df from csv

    if language == "fr":
        # reduce dataset to smaller subset of unique indicator codes (less mem required)
        df["IndicatorCode"] = build_indicator_code(df["COORDONNÉES"],
                                                   df["PÉRIODE DE RÉFÉRENCE"], prod_id)
        df.drop(["COORDONNÉES"], axis=1, inplace=True)  # not nec. after IndicatorCode built
        df.drop_duplicates(subset=["IndicatorCode"], inplace=True)
        df.rename(columns={"UNITÉ DE MESURE": "UOM_FR"}, inplace=True)  # to match db

    elif language == "en":
        # english dataset is much larger, do more prelim processing
        df["IndicatorCode"] = build_indicator_code(df["COORDINATE"], df["REF_DATE"], prod_id)
        df.drop(["COORDINATE"], axis=1, inplace=True)  # not nec. after IndicatorCode built
        df.rename(columns={"VECTOR": "Vector", "UOM": "UOM_EN"}, inplace=True)  # to match db
        df["DGUID"] = df["DGUID"].str.replace(".", "").str.replace("201A", "2015A")  # vintage
        df["RefYear"] = df["REF_DATE"].map(h.fix_ref_year).astype("string")  # need 4 digit year
        df["IndicatorThemeID"] = prod_id
        df["ReleaseIndicatorDate"] = rel_date
        df["ReferencePeriod"] = df["RefYear"] + "-01-01"  # becomes Jan 1
        df["ReferencePeriod"] = df["ReferencePeriod"].astype("datetime64[ns]")
        df["Vector"] = df["Vector"].str.replace("v", "").astype("int32")  # remove v, make int

    return df


def set_uom_format(uom_id, lang):
    # returns format string for specified uom_id and language (lang)
    loc_code = "en-US"
    if lang == "fr":
        loc_code = "fr-CA"

    format_str = "Format(iv.value, 'N', '" + loc_code + "')"  # default
    if uom_id == 223:
        format_str = "Format(iv.value, 'N0', '" + loc_code + "')"
    elif uom_id == 81:
        format_str = "Format(iv.value, 'C0', '" + loc_code + "')"
    elif uom_id == 239:
        format_str = "Format(iv.value/100, 'P1', '" + loc_code + "')"
    return format_str
