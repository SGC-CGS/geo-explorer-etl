# for handling CLI arguments
import argparse
from datetime import date
import logging
import sys

# set up logger if available
log = logging.getLogger("etl_log")
log.addHandler(logging.NullHandler())


class argParser(object):
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("-i", dest="insert_new_table", action="store_true", help="Insert Product ID as a new "
                                    "product. If flag is absent, existing product data will be updated.")
        self.parser.add_argument("--start", type=date.fromisoformat, metavar="YYYY-MM-DD",
                                 help="Start release date when looking for product updates.")
        self.parser.add_argument("--end", type=date.fromisoformat,  metavar="YYYY-MM-DD",
                                 help="End release date when looking for product updates.")
        self.parser.add_argument("--prodid", type=int, nargs='*',  metavar="PRODID_1 PRODID_2",
                                 help="Product ID to create or update (no special characters). If inserting a new "
                                      "product, several Product IDs can be specified (separated by spaces) to merge "
                                      "into a single product. If products are merged, the first Product ID entered "
                                      "will become the new Indicator Theme ID for all merged data. All merged products "
                                      "must have the same dimension/member structure.")
        self.args = self.parser.parse_args()

    def check_valid_parse_args(self):
        # check the parsed arguments to see if they are valid. Returns status message if issues, otherwise "".
        # Note data type validation is done when the argument is created.
        ret_msg = ""
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
            elif len(self.args.prodid) > 1:
                ret_msg = "Multiple Product IDs can only be used if creating a new merged product with the -i flag."
            elif not self.args.start and not self.args.end and not self.args.prodid:
                ret_msg = "No arguments were received."
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