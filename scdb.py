# Database class
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
                "(" + pid_subqry + "))"
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
            print("Could not delete product from database. See detailed mssage below:")
            print(str(err))
        else:
            self.cursor.commit()
            retval = True
            print("Successfully deleted product.")
        return retval

    def execute_simple_select_query(self, query):
        # execute a simple select query (no criteria) and return all results
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        return results

    def get_last_dimension_id(self):
        # returns h
        # ighest dimension id in db
        query = "SELECT MAX(DimensionId) FROM gis.Dimensions"
        results = self.execute_simple_select_query(query)
        retval = False
        if len(results) == 1:
            retval = results[0][0]
        return retval

    def get_last_dimension_value_id(self):
        # returns highest dimension value id in db
        query = "SELECT MAX(DimensionValueId) FROM gis.DimensionValues"
        results = self.execute_simple_select_query(query)
        retval = False
        if len(results) == 1:
            retval = results[0][0]
        return retval

    def get_last_indicator_id(self):
        # returns highest indicator id in db
        query = "SELECT MAX(IndicatorId) FROM gis.Indicator"
        results = self.execute_simple_select_query(query)
        retval = 0  # if it stays 0 the table is empty
        if len(results) == 1:
            retval = results[0][0]
        return retval

    def get_last_indicator_value_id(self):
        # returns highest indicator values id in db
        query = "SELECT MAX(IndicatorValueId) FROM gis.IndicatorValues"
        results = self.execute_simple_select_query(query)
        retval = False
        if len(results) == 1:
            retval = results[0][0]
        return retval

    def get_matching_product_list(self, changed_products):
        # returns products in that match changed_products list
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

    def update_dimension(self):  # TEST FUNCTION
        # update dimension data
        query = "UPDATE gis.Dimensions SET Dimension_EN = ? WHERE DimensionId = ?"
        self.cursor.execute(query, 'Date', 6)
        self.connection.commit()

    def insert_dimension(self, dim_rows):  # TEST FUNCTION
        # insert dimension data
        # find last identity column
        cur_dim_id = self.get_last_dimension_id()

        if cur_dim_id:
            # increment dim id for each insert
            for dim_row in dim_rows:
                cur_dim_id += 1
                dim_row[0] = cur_dim_id  # updates id for each row
                print("Inserting: " + str(dim_row))

            # do the insert
            query = "INSERT INTO gis.Dimensions(DimensionId, IndicatorThemeId, Dimension_EN, Dimension_FR, " \
                    "DisplayOrder, DimensionType) " \
                    "VALUES(?, ?, ?, ?, ?, ?)"
            self.cursor.executemany(query, dim_rows)
            self.connection.commit()
            return True
        else:
            return False

    def insert_dimension_values(self, dim_rows):  # TEST FUNCTION
        # insert dimension values data
        # find last identity column
        cur_dim_val_id = self.get_last_dimension_value_id()

        if cur_dim_val_id:
            # increment dim value id for each insert
            for dim_row in dim_rows:
                cur_dim_val_id += 1
                dim_row[0] = cur_dim_val_id  # updates id for each row
                print("Inserting: " + str(dim_row))

            # do the insert
            query = "INSERT INTO gis.DimensionValues(DimensionValueId, DimensionId, Display_EN, " \
                    "Display_FR, ValueDisplayOrder, ValueDisplayParent) " \
                    "VALUES(?, ?, ?, ?, ?, ?)"
            self.cursor.executemany(query, dim_rows)
            self.connection.commit()
            return True
        else:
            return False

    def vector_and_ref_period_match(self, product_id, vector_id, reference_period):
        # find match for specified product, vector and reference period
        # Note we first want to know if the vector matches, and THEN
        #   if the reference period matches. This is done by getting
        #   all reference periods for a vector and then searching the
        #   result for the specified reference period.
        # Return Values:
        #   VectorFound --> only a matching vector was found
        #   VectorRefPeriodFound --> matching vector and reference period were both found
        #   NotFound --> no match was found
        retval = "NotFound"
        query = "SELECT ReferencePeriod FROM gis.Indicator WHERE IndicatorThemeId = ? AND Vector = ?"
        self.cursor.execute(query, int(product_id), int(vector_id))

        results = self.cursor.fetchall()
        if len(results) > 0:
            retval = "VectorFound"
            for res in results:
                if res[0].strftime("%Y-%m-%d") == reference_period:
                    retval = "VectorRefPeriodFound"

        return retval
