# data frame handling

import helpers as h  # helper functions
import numpy as np
import pandas as pd


def build_column_and_type_dict(dimensions, lang):
    # set up the dicionary of columns and data types for pandas df
    # add columns listed in dimensions to the predefined columns below.
    # returns dictionary of cols:types for en and fr
    # Note:
    #   df returns all strings as object type by default
    #   category more efficient for string fields if there are < 50% unique values and no string operations are required
    if lang == "en":
        cols = {"REF_DATE": "string", "DGUID": "string", "UOM": "category", "VECTOR": "string", "COORDINATE": "string",
                "STATUS": "category", "SYMBOL": "string", "TERMINATED": "category", "VALUE": "float64"}
    else:
        cols = {"PÉRIODE DE RÉFÉRENCE": "string", "COORDONNÉES": "string", "UNITÉ DE MESURE": "category"}

    for dim in dimensions[lang]:
        cols[dim] = "string"
    return cols


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

    # TODO: NOTE - Production database shows that a GeographicLevelId of "SSSS" is added to
    #  gis.GeographicLevelForIndicator for every IndicatorId. These rows are apparently used for
    #  the select drop down box on the web site. This part of the process is not in the PowerBI or
    #  SSIS packages that were supplied. Need to confirm when these rows are added (i.e., is it
    #  added manually with SQL statements after the PowerBI and SSIS packages are run?) and whether
    #  this modification should be included here.

    print("Finished building GeohraphicLevelForIndicator table.")
    return df_gli


def build_indicator_code(coordinate, reference_date, pid_str):
    # build a custom indicator code that strips geography from the coordinate
    # and adds a reference date
    # IndicatorCode ex. 13100778.1.23.1.2017/2018-01-01
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
        # field lengths
        df["IndicatorName_" + ln] = df["IndicatorName_" + ln].str[:1000]  # str
        df["IndicatorDisplay_" + ln] = df["IndicatorDisplay_" + ln].str[:500]  # str
        df["UOM_" + ln] = df["UOM_" + ln].astype("string").str[:50]  # str
        df["IndicatorNameLong_" + ln] = df["IndicatorNameLong_" + ln].str[:1000]  # str

    df["ReleaseIndicatorDate"] = df["ReleaseIndicatorDate"].astype("datetime64[ns]")
    df["IndicatorCode"] = df["IndicatorCode"].str[:100]

    # Keep only the columns needed for insert
    df = df.loc[:, ["IndicatorId", "IndicatorName_EN", "IndicatorName_FR", "IndicatorThemeID",
                    "ReleaseIndicatorDate", "ReferencePeriod", "IndicatorCode", "IndicatorDisplay_EN",
                    "IndicatorDisplay_FR", "UOM_EN", "UOM_FR", "Vector", "IndicatorNameLong_EN",
                    "IndicatorNameLong_FR"]]

    print("Finished building Indicator table.")
    return df


def build_indicator_values_df(edf, gdf, ndf, next_id):
    # build the data frame for IndicatorValues
    # based on dataframe of english csv file (edf), GeographyReference ids (gdf), and NullReason ids (ndf).
    # populate indicator value ids starting from next_id.

    print("Building IndicatorValues table.")
    df_iv = edf.loc[:, ["DGUID", "IndicatorCode", "STATUS", "VALUE"]]  # subset of full en dataset
    df_iv["IndicatorValueId"] = create_id_series(edf, next_id)  # populate IDs

    df_iv = pd.merge(df_iv, gdf, left_on="DGUID", right_on="GeographyReferenceId", how="left")  # join to geoRef for id
    df_iv.dropna(subset=["GeographyReferenceId"], inplace=True)  # drop empty ids
    df_iv.drop(["GeographyReferenceId"], axis=1, inplace=True)

    # TODO: The step aboves removes any rows that do not have a matching DGUID in the GeographyReference table.
    #  This step exists in the PowerBI process and results in many IndicatorValues being removed from the dataset.
    #  It is not currently clear how the GeographyReference table gets updated with new DGUIDs.

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
        df["ReferencePeriod"] = df["RefYear"] + "-01-01"
        df["ReferencePeriod"] = df["ReferencePeriod"].astype("datetime64[ns]")  # becomes Jan 1
        df["Vector"] = df["Vector"].str.replace("v", "").astype("int32")  # remove v, make int

    return df
