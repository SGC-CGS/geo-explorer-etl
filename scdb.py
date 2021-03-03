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
        # execute a simple select query and return a single result, or false if no values
        retval = False
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        if len(results) == 1:
            retval = results[0][0]
        return retval

    def get_date_dimension_values(self, pid):
        # return date dimension values from gis.DimensionValues as a pandas dataframe for specified product (pid)
        query = "SELECT DimensionValueId, DimensionId, Display_EN, Display_FR, ValueDisplayOrder FROM " \
                "gis.DimensionValues WHERE DimensionId IN (SELECT DimensionId FROM gis.Dimensions WHERE " \
                "IndicatorThemeId = ? AND Dimension_EN='Date')"
        retval = pd.read_sql(query, self.connection, params=[pid])
        return retval

    def get_date_dimension_id_for_product(self, pid):
        # return the DimensionId for the false "Date" dimension for specified product (pid)
        query = "SELECT DimensionId FROM gis.Dimensions WHERE IndicatorThemeId = " + pid + " AND Dimension_EN='Date'"
        retval = self.execute_simple_select_query(query)
        return retval

    def get_dimensions_and_members_by_product(self, pid):
        # return dimensions and members from gis.Dimensions and gis.DimensionValues as a pandas dataframe
        query = "SELECT dv.DimensionValueId, dv.DimensionId, dv.Display_EN, dv.ValueDisplayOrder, " \
                "dv.ValueDisplayParent, d.IndicatorThemeId, d.Dimension_EN, d.DisplayOrder " \
                "FROM gis.DimensionValues as dv INNER JOIN gis.Dimensions as d " \
                "ON dv.DimensionId = d.DimensionId " \
                "WHERE d.IndicatorThemeId = ? " \
                "ORDER BY DisplayOrder, ValueDisplayOrder"
        retval = pd.read_sql(query, self.connection, params=[pid])
        return retval

    def get_geo_reference_ids(self):
        # return all ids from gis.GeographyReference as a pandas dataframe
        query = "SELECT GeographyReferenceId FROM gis.GeographyReference"
        retval = pd.read_sql(query, self.connection)
        return retval

    def get_indicator_chart_info(self, pid):
        # return chart information from gis.IndicatorMetaData and gis.RelatedCharts for the specified product (pid)
        query = "SELECT i.IndicatorThemeId, i.IndicatorCode, im.DefaultBreaksAlgorithmId, im.DefaultBreaks, " \
                "im.PrimaryChartTypeId ,im.ColorTo ,im.ColorFrom, r.ChartTypeId, r.ChartTitle_EN, r.ChartTitle_FR, " \
                "r.FieldAlias_EN, r.FieldAlias_FR, r.Query FROM gis.Indicator AS i LEFT JOIN gis.IndicatorMetaData " \
                "AS im ON i.IndicatorId=im.IndicatorId LEFT JOIN gis.RelatedCharts AS r ON im.IndicatorId = " \
                "r.RelatedChartId WHERE IndicatorThemeId = ? "
        retval = pd.read_sql(query, self.connection, params=[pid])
        return retval

    def get_indicator_null_reason(self):
        # return all rows from gis.IndicatorNullReason as a pandas dataframe
        query = "SELECT NullReasonId, Symbol FROM gis.IndicatorNullReason WHERE Symbol IS NOT NULL"
        retval = pd.read_sql(query, self.connection)
        return retval

    def get_last_date_dimension_display_order(self, dim_id):
        # return last ValueDisplayOrder value for the specified dimension id (dim_id), 0 if none found
        query = "SELECT MAX(ValueDisplayOrder) FROM gis.DimensionValues WHERE DimensionId = ?"
        self.cursor.execute(query, dim_id)
        results = self.cursor.fetchall()
        retval = results[0][0] if len(results) == 1 else None  # store result
        retval = 0 if retval is None else retval  # reset to 0 if no value
        return retval

    def get_last_table_id(self, id_field_name, table_name, schema_name):
        # return highest id for specified field in table (schema_name, table_name, id_field_name), or false if none
        query = "SELECT MAX(" + id_field_name + ") FROM " + schema_name + "." + table_name
        retval = self.execute_simple_select_query(query)
        retval = 0 if retval is None else retval
        return retval

    def get_matching_product_list(self, product_list):
        # returns list of product ids that match product_list
        retval = []
        if product_list and len(product_list) > 0:
            in_clause = ', '.join(map(str, product_list))  # flatten list
            query = "SELECT DISTINCT IndicatorThemeID FROM gis.IndicatorTheme WHERE IndicatorThemeID IN (" + \
                    in_clause + ")"
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
