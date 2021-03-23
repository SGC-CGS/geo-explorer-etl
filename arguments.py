# for handling CLI arguments
import argparse
from datetime import date
import logging
import sys

# set up logger if available
log = logging.getLogger("etl_log")
log.addHandler(logging.NullHandler())


def show_merge_warning(sib_prod_id, master_prod_id, json_file_name):
    # create warning message for incorrect operations on merged tables
    sib_prod_id = str(sib_prod_id)
    master_prod_id = str(master_prod_id)
    if sib_prod_id != "":
        msg = "Product " + sib_prod_id + " is a sibling table in a merged product (Master: " + master_prod_id + \
              ") and cannot be inserted alone nor can it be updated automatically in a date range. See " + \
              json_file_name + " for details.\n"
    else:
        msg = "Product " + master_prod_id + " is the master table in a merged product and cannot be inserted alone " \
              "nor can it be updated automatically in a date range. See " + json_file_name + " for details.\n"
    return msg


class argParser(object):
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("-i", dest="insert_new_table", action="store_true", help="Insert Product ID as a new "
                                    "product. If flag is absent, existing product data will be updated.")
        self.parser.add_argument("--start", type=date.fromisoformat, metavar="YYYY-MM-DD",
                                 help="Start release date when looking for product updates from get_changed_cube_list.")
        self.parser.add_argument("--end", type=date.fromisoformat,  metavar="YYYY-MM-DD",
                                 help="End release date when looking for product updates from get_changed_cube_list.")
        self.parser.add_argument("--prodid", type=int, nargs='*',  metavar="PRODID_1 PRODID_2",
                                 help="Product ID to insert or update (no special characters). If inserting a new "
                                      "product, several Product IDs can be specified (separated by spaces) to merge "
                                      "into a single product. If products are merged, the first Product ID entered "
                                      "will become the new Indicator Theme ID for all merged data. All merged products "
                                      "must have the same dimension/member structure.")
        self.parser.add_argument("--minrefyear", type=int, metavar="YYYY",
                                 help="Earliest reference date to process from data file. Example: --minrefyear 2017 "
                                      "will only add data with a reference date >= 2017-01-01.")

        self.args = self.parser.parse_args()

    def check_valid_parse_args(self):
        # check the parsed arguments to see if they are valid. Returns status message if issues, otherwise "".
        # Note data type validation is done when the argument is created.
        ret_msg = ""
        if self.args.minrefyear:
            if len(str(self.args.minrefyear)) != 4:
                ret_msg = "Minimum reference year must be a 4 digit number."

        if self.args.insert_new_table:
            # arguments for inserting a new product
            if not self.args.prodid:
                ret_msg = "Product ID is required for new products created with the -i flag."
        else:
            # arguments for append
            if self.args.start and self.args.end and not self.args.prodid:
                if self.args.end < self.args.start:
                    ret_msg = "Start date must be before end date. Please check the date parameters and try again."
            elif self.args.prodid and (self.args.start or self.args.end):
                ret_msg = "Product ID search cannot be combined with start/end dates."
            elif (self.args.start and not self.args.end) or (not self.args.start and self.args.end):
                ret_msg = "Start and end date must both be present to look up products within a date range."
            elif self.args.prodid is not None and len(self.args.prodid) > 1:
                ret_msg = "Multiple Product IDs can only be used if creating a new merged product with the -i flag."
            elif not self.args.start and not self.args.end and not self.args.prodid:
                ret_msg = "Not enough arguments were received. At a minimum, --prodid OR --start and --end must be " \
                          "included."
        return ret_msg

    def get_arg_value(self, arg_name):
        ret_val = getattr(self.args, arg_name, False)
        ret_val = ret_val if ret_val is not None else False  # if none reset to false
        return ret_val

    def show_help_and_exit_with_msg(self, msg):
        # log message (msg) and exit the program
        log.error("Error: " + msg)
        self.parser.print_help()
        sys.exit()
