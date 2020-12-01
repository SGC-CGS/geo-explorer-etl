# Database class
import datetime
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
        self.connection = pyodbc.connect(conn_string)
        self.cursor = self.connection.cursor()

    def execute_simple_select_query(self, query):
        # execute a simple select query (no criteria) and return all results
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        return results

    def get_last_dimension_id(self):
        # returns highest dimension id in db
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
        retval = False
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

    def vector_and_ref_period_match(self, vector_id, reference_period):
        # find match for specified vector and reference period
        vector_id = int(vector_id)
        if len(str(reference_period)) == 4:  # if only year is given, set to Jan 1 for db
            reference_period = datetime.date(reference_period, 1, 1)
        reference_period = reference_period.strftime("%Y-%m-%d")

        retval = False
        query = "SELECT COUNT(*) FROM gis.Indicator WHERE ReferencePeriod = ? AND Vector = ?"
        self.cursor.execute(query, reference_period, int(vector_id))

        results = self.cursor.fetchall()
        if len(results) == 1:
            retval = results[0][0]
        return retval
