# Database class
import logging
import pandas as pd
import pyodbc
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy import exc

# set up logger if available
log = logging.getLogger("etl_log")
log.addHandler(logging.NullHandler())


# noinspection SpellCheckingInspection
class sqlDb(object):
    def __init__(self, driver, server, database):
        # set up db configuration and open a connection
        self.driver = driver
        self.server = server
        self.database = database
        conn_string = "Driver={" + self.driver + "};Server=" + self.server + ";Trusted_Connection=yes;Database=" + \
                      self.database + ";"
        log.info("Connecting to DB: " + conn_string)
        self.connection = pyodbc.connect(conn_string, autocommit=False)
        self.cursor = self.connection.cursor()

        # sql alchemy engine for bulk inserts
        sa_params = urllib.parse.quote(conn_string)
        log.info("Setting up SQL Alchemy engine.\n")
        self.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % sa_params, fast_executemany=True)

    def delete_product(self, product_id):
        # Delete queries are in order as described in confluence document for deleting a product
        # Note: We are not deleting the data from Dimensions, DimensionValues, or IndicatorTheme
        retval = False
        pid = str(product_id)
        log.info("Deleting product " + pid + " from database.")
        log.info("Note: Data will NOT be deleted from Dimensions, DimensionValues, or IndicatorTheme.")
        pid_subqry = "SELECT IndicatorId FROM gis.Indicator WHERE IndicatorThemeId = ? "
        qry1 = "DELETE FROM gis.RelatedCharts WHERE RelatedChartId IN (" + pid_subqry + ") "
        qry2 = "DELETE FROM gis.IndicatorMetaData WHERE IndicatorId IN (" + pid_subqry + ") "
        qry3 = "DELETE FROM gis.IndicatorValues WHERE IndicatorValueId IN (" \
               "SELECT IndicatorValueId FROM gis.GeographyReferenceForIndicator WHERE IndicatorId IN " \
               "(" + pid_subqry + ")) "  # OR IndicatorValueCode like '%" + pid + "%'"  # added 2nd clause
        qry4 = "DELETE FROM gis.GeographyReferenceForIndicator WHERE IndicatorId in (" + pid_subqry + ") "
        qry5 = "DELETE FROM gis.GeographicLevelForIndicator WHERE IndicatorId in (" + pid_subqry + ") "
        qry6 = "DELETE FROM gis.Indicator WHERE IndicatorThemeId = ?"

        try:
            log.info("Deleting from gis.RelatedCharts.")
            self.cursor.execute(qry1, pid)
            log.info("Deleting from gis.IndicatorMetaData.")
            self.cursor.execute(qry2, pid)
            log.info("Deleting from gis.IndicatorValues.")
            self.cursor.execute(qry3, pid)
            log.info("Deleting from gis.GeographyReferenceForIndicator.")
            self.cursor.execute(qry4, pid)
            log.info("Deleting from gis.GeographyLevelForIndicator.")
            self.cursor.execute(qry5, pid)
            log.info("Deleting from gis.Indicator.")
            self.cursor.execute(qry6, pid)
        except pyodbc.Error as err:
            self.cursor.rollback()
            log.error("Could not delete product from database. See detailed message below:")
            log.error(str(err))
        else:
            self.cursor.commit()
            retval = True
            log.info("Successfully deleted product.\n")
        return retval

    def execute_simple_select_query(self, query):
        # execute a simple select query (no criteria) and return a single result, or false if no values
        retval = False
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        if len(results) == 1:
            retval = results[0][0]
        return retval

    def get_dimensions_and_members_by_product(self, pid):
        # return dimensions and members from gis.Dimensions and gis.DimensionValues as a pandas dataframe
        query = "SELECT dv.DimensionValueId, dv.DimensionId, dv.Display_EN, dv.ValueDisplayOrder, " \
                "dv.ValueDisplayParent, d.IndicatorThemeId, d.Dimension_EN, d.DisplayOrder " \
                "FROM gis.DimensionValues as dv INNER JOIN gis.Dimensions as d " \
                "ON dv.DimensionId = d.DimensionId " \
                "WHERE d.IndicatorThemeId = " + pid + \
                "ORDER BY DisplayOrder, ValueDisplayOrder"
        retval = pd.read_sql(query, self.connection)
        return retval

    def get_geo_reference_ids(self):
        # return all ids from gis.GeographyReference as a pandas dataframe
        query = "SELECT GeographyReferenceId FROM gis.GeographyReference"
        retval = pd.read_sql(query, self.connection)
        return retval

    def get_indicator_null_reason(self):
        # return all rows from gis.IndicatorNullReason as a pandas dataframe
        query = "SELECT NullReasonId, Symbol FROM gis.IndicatorNullReason WHERE Symbol IS NOT NULL"
        retval = pd.read_sql(query, self.connection)
        return retval

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

    def insert_dataframe_rows(self, df, table_name, schema_name):
        # insert dataframe (df) to the database for schema (schema_name) and table (table_name)
        try:
            df.to_sql(name=table_name, con=self.engine, schema=schema_name, if_exists="append", index=False,
                      chunksize=10000)  # make sure to use default method=None
        except (pyodbc.Error, exc.SQLAlchemyError) as err:
            log.error("Could not insert to database for table: " + schema_name + "." + table_name +
                                                             ". See detailed message below:")
            log.error(str(err) + "\n")
            raise Exception(str(err))
        else:
            ret_val = True

        return ret_val
