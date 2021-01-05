# Database class
import pandas as pd
import pyodbc


# noinspection SpellCheckingInspection
class sqlDb(object):
    def __init__(self, driver, server, database):
        # set up db configuration and open a connection
        self.driver = driver
        self.server = server
        self.database = database
        conn_string = "Driver={" + self.driver + "};Server=" + self.server + \
                      ";Trusted_Connection=yes;Database=" + self.database + ";"
        print("Connecting to DB: " + conn_string)
        self.connection = pyodbc.connect(conn_string, autocommit=False)
        self.cursor = self.connection.cursor()

    def delete_product(self, product_id):
        # Delete queries are in order as described in confluence document for deleting a product
        # Note: We are not deleting the data from Dimensions, DimensionValues, or IndicatorTheme
        retval = False
        pid = str(product_id)
        print("\nDeleting product " + pid + " from database...")
        print("Note: Data will NOT be deleted from Dimensions, DimensionValues, or IndicatorTheme.")
        pid_subqry = "SELECT IndicatorId FROM gis.Indicator WHERE IndicatorThemeId = ? "
        qry1 = "DELETE FROM gis.RelatedCharts WHERE RelatedChartId IN (" + pid_subqry + ") "
        qry2 = "DELETE FROM gis.IndicatorMetaData WHERE IndicatorId IN (" + pid_subqry + ") "
        qry3 = "DELETE FROM gis.IndicatorValues WHERE IndicatorValueId IN (" \
               "SELECT IndicatorValueId FROM gis.GeographyReferenceForIndicator WHERE IndicatorId IN " \
               "(" + pid_subqry + ")) OR IndicatorValueCode like '%" + pid + "%'"  # added 2nd clause to confluence code
        qry4 = "DELETE FROM gis.GeographyReferenceForIndicator WHERE IndicatorId in (" + pid_subqry + ") "
        qry5 = "DELETE FROM gis.GeographicLevelForIndicator WHERE IndicatorId in (" + pid_subqry + ") "
        qry6 = "DELETE FROM gis.Indicator WHERE IndicatorThemeId = ?"

        try:
            self.cursor.execute(qry1, pid)
            self.cursor.execute(qry2, pid)
            self.cursor.execute(qry3, pid)
            self.cursor.execute(qry4, pid)
            self.cursor.execute(qry5, pid)
            self.cursor.execute(qry6, pid)
        except pyodbc.Error as err:
            self.cursor.rollback()
            print("Could not delete product from database. See detailed message below:")
            print(str(err))
        else:
            self.cursor.commit()
            retval = True
            print("Successfully deleted product.")
        return retval

    def execute_simple_select_query(self, query):
        # execute a simple select query (no criteria) and return a single result, or false if no values
        retval = False
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        if len(results) == 1:
            retval = results[0][0]
        return retval

    def get_geo_reference_ids(self):
        # return all ids from gis.GeographyReference as a pandas dataframe
        query = "SELECT GeographyReferenceId FROM gis.GeographyReference"
        results = pd.read_sql(query, self.connection)
        return results

    def get_indicator_null_reason(self):
        # return all rows from gis.IndicatorNullReason as a pandas dataframe
        query = "SELECT NullReasonId, Symbol FROM gis.IndicatorNullReason WHERE Symbol IS NOT NULL"
        results = pd.read_sql(query, self.connection)
        return results

    def get_last_indicator_id(self):
        # returns highest indicator id in db, or false if none
        query = "SELECT MAX(IndicatorId) FROM gis.Indicator"
        retval = self.execute_simple_select_query(query)
        return retval

    def get_last_indicator_value_id(self):
        # returns highest indicator values id in db, or false if none
        query = "SELECT MAX(IndicatorValueId) FROM gis.IndicatorValues"
        retval = self.execute_simple_select_query(query)
        return retval

    def get_matching_product_list(self, changed_products):
        # returns list of product ids that match changed_products list
        retval = []
        if len(changed_products) > 0:
            in_clause = ', '.join(map(str, changed_products))  # flatten list
            query = "SELECT DISTINCT IndicatorThemeID FROM gis.IndicatorTheme " \
                    "WHERE IndicatorThemeID IN (" + in_clause + ")"
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            for prod in results:
                retval.append(prod[0])
        return retval

    def get_pid_indicators_as_df(self, product_id):
        # return the indicators for the specified product_id as a pandas dataframe
        query = "SELECT IndicatorId, IndicatorCode FROM gis.Indicator WHERE IndicatorThemeId = ?"
        results = pd.read_sql(query, self.connection, params=[product_id])
        return results

    def insert_geography_level_for_indicator(self, df):
        # setup gis.GeographyLevelForIndicator query from dataframe and call insert. Returns num of rows inserted.
        print("Inserting to gis.GeographyLevelForIndicator... ")
        qry = "INSERT INTO gis.GeographicLevelForIndicator (IndicatorId, GeographicLevelId) VALUES (?,?)"
        inserted = self.insert_dataframe_rows(qry, df)
        return inserted

    def insert_geography_reference_for_indicator(self, df):
        # setup gis.GeographyReferenceForIndicator query from dataframe and call insert. Returns num of rows inserted.
        print("Inserting to gis.GeographyReferenceForIndicator... ")
        qry = "INSERT INTO gis.GeographicReferencelForIndicator (GeographyReferenceId, IndicatorId, " \
              "IndicatorValueId, ReferencePeriod) VALUES (?,?,?,?)"
        inserted = self.insert_dataframe_rows(qry, df)
        return inserted

    def insert_indicator(self, df):
        # setup gis.Indicator query from dataframe and call insert. Returns num of rows inserted.
        print("Inserting to gis.Indicator... ")
        qry = "INSERT INTO gis.Indicator (IndicatorId, IndicatorName_EN, IndicatorName_FR, IndicatorThemeID, " \
              "ReleaseIndicatorDate, ReferencePeriod, IndicatorCode, IndicatorDisplay_EN, IndicatorDisplay_FR, " \
              "UOM_EN, UOM_FR, Vector, IndicatorNameLong_EN, IndicatorNameLong_FR) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        inserted = self.insert_dataframe_rows(qry, df)
        return inserted

    def insert_indicator_metadata(self, df):
        # setup gis.IndicatorMetaData query from dataframe and call insert. Returns num of rows inserted.
        print("Inserting to gis.IndicatorMetadata... ")
        qry = "INSERT INTO gis.IndicatorMetadata (MetaDataId, IndicatorId, FieldAlias_EN, FieldAlias_FR, " \
              "DataFormatId, DefaultBreaksAlgorithmId, DefaultBreaks, PrimaryChartTypeId, PrimaryQuery, ColorTo, " \
              "ColorFrom, DimensionUniqueKey, DefaultRelatedChartId) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
        inserted = self.insert_dataframe_rows(qry, df)
        return inserted

    def insert_indicator_values(self, df):
        # set up gis.IndicatorValues query from data frame and call insert. Returns number of rows inserted.
        print("Inserting to gis.IndicatorValues... ")
        qry = "INSERT INTO gis.IndicatorValues (IndicatorValueId, Value, NullReasonId, IndicatorValueCode) " \
              "values (?,?,?,?)"
        inserted = self.insert_dataframe_rows(qry, df)
        return inserted

    def insert_related_charts(self, df):
        # set up gis.RelatedCharts query from data frame and call insert. Returns number of rows inserted.
        print("Inserting to gis.RelatedCharts... ")
        qry = "INSERT INTO gis.RelatedCharts (RelatedChartId, ChartTitle_EN, ChartTitle_FR, Query, ChartTypeId, " \
              "IndicatorMetaDataId, DataFormatId, FieldAlias_EN, FieldAlias_FR) values (?,?,?,?,?,?,?,?,?)"
        inserted = self.insert_dataframe_rows(qry, df)
        return inserted

    def insert_dataframe_rows(self, qry, df):
        # insert an entire dataframe (df) to the database based on specified query (qry)
        # returns number of rows inserted (inserted)
        inserted = 0

        self.cursor.fast_executemany = True
        for row_count in range(0, df.shape[0]):
            chunk = df.iloc[row_count:row_count + 1, :].values.tolist()
            tuple_of_tuples = tuple(tuple(x) for x in chunk)  # tuple required for pyodbc fast insert
            try:
                self.cursor.executemany(qry, tuple_of_tuples)
            except pyodbc.Error as err:
                self.cursor.rollback()
                print("Could not insert row to database. See detailed mssage below:")
                print(str(err) + "\n" + str(tuple_of_tuples) + "\n")
            else:
                self.cursor.commit()
                inserted += 1
        print("Inserted " + str(inserted) + " records.\n")
        return inserted
