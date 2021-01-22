# WDS class
from datetime import datetime
import logging
import requests

# set up logger if available
log = logging.getLogger("etl_log")
log.addHandler(logging.NullHandler())


def get_metadata_dimension_names(metadata, ignore_geo):
    # return list of dimensions from meta data
    #   metadata - meta data formatted as it comes from from get_cube_metadata
    #   ignore_geo - True=skip geography dimension
    dim = {"en": [], "fr": []}
    if "dimension" in metadata:
        for dimension in metadata["dimension"]:
            en_name = dimension["dimensionNameEn"].upper()
            if en_name != "GEOGRAPHY" or (not ignore_geo and en_name == "GEOGRAPHY"):
                dim["en"].append(dimension["dimensionNameEn"])
                dim["fr"].append(dimension["dimensionNameFr"])
    else:
        log.warning("Could not find any dimensions in the metadata.")
    return dim


def get_metadata_field(metadata, field_name, default_value):
    # return requested field (field_name) from get cube metadata results (metadata)
    # if not available, return default (default_value)
    retval = default_value
    if field_name in metadata:
        retval = metadata[field_name]
    else:
        print("Could not release find " + field_name + " in the cube metadata.")
    return retval


def get_metadata_release_date(metadata):
    # return release date from meta data
    #   metadata - meta data formatted as it comes from from get_cube_metadata
    retval = datetime.today().isoformat(timespec="minutes")  # ex. 2020-12-11T10:11
    if "releaseTime" in metadata:
        retval = metadata["releaseTime"]
    else:
        log.warning("Could not release date/time in the metadata. Setting to current.")
    return retval


def write_file(filename, content, flags):
    retval = False
    try:
        with open(filename, flags) as f:
            f.write(content)
    except IOError as e:
        log.warning("Failed saving to " + filename)
        log.warning("Error: File could not be written to disk. \n" + str(e))
    else:
        log.info("File saved to " + filename)
        retval = True
    return retval


class serviceWds(object):
    def __init__(self, wds_url, delta_url):
        self.wds_url = wds_url
        self.delta_url = delta_url
        self.last_http_req_status = False

        # code sets
        self.code_sets = {}
        self.scalar_codes = {}
        self.frequency_codes = {}
        self.symbol_codes = {}
        self.status_codes = {}
        self.uom_codes = {}
        self.survey_codes = {}
        self.subject_codes = {}
        self.classification_type_codes = {}
        self.security_level_codes = {}
        self.terminated_codes = {}
        self.wds_response_status_codes = {}
        self.get_code_sets()  # retrieved once only

    def check_http_request_status(self, r):
        # Verify http request status.
        # note some services will return 404 if there is no data available, this doesn't handle that yet.
        if r.status_code == requests.codes.ok:
            self.last_http_req_status = True
        else:
            log.warning("Could not access WDS because of error: " + str(r.status_code))
            r.raise_for_status()
            self.last_http_req_status = False
        return

    def check_wds_response_status_code(self, response_code):
        # wds returns a response code for each line item (ex. self.check_wds_response_status_code(8))
        # function returns 0 if Success, status message if anything else
        retval = 0
        if response_code != 0:
            retval = next(
                (str(row["codeId"]) + " - " + row["codeTextEn"] + " / " + row["codeTextFr"]
                 for row in self.wds_response_status_codes
                 if row['codeId'] == response_code), None)
        return retval

    def get_changed_cube_list(self, str_date):
        # submits WDS request, returns list of product ids for date
        # str_date - YYYY-MM-DD
        url = self.wds_url + "getChangedCubeList" + "/" + str_date
        log.info("Accessing " + url)
        r = requests.get(url)
        self.check_http_request_status(r)

        retval = False
        if self.last_http_req_status:
            resp = r.json()
            if resp["status"] != "SUCCESS":
                log.warning("Changed cube list could not be retrieved. WDS returned: " + str(resp["status"]) +
                            " for Date " + str_date)
            else:
                prod_list = []
                for row in resp["object"]:
                    prod_list.append(row["productId"])
                retval = prod_list

        return retval

    def get_code_sets(self):
        # submits WDS request, returns list of code sets
        url = self.wds_url + "getCodeSets"
        log.info("Retrieving code sets from " + url)
        r = requests.get(url)
        self.check_http_request_status(r)

        retval = False
        if self.last_http_req_status:
            resp = r.json()
            if resp["status"] != "SUCCESS":
                log.warning("Code set list could not be retrieved. WDS returned: " + str(resp["status"]))
            else:
                for set_type in resp["object"]:
                    if set_type == "scalar":
                        self.scalar_codes = resp["object"][set_type]
                    elif set_type == "frequency":
                        self.frequency_codes = resp["object"][set_type]
                    elif set_type == "symbol":
                        self.symbol_codes = resp["object"][set_type]
                    elif set_type == "status":
                        self.status_codes = resp["object"][set_type]
                    elif set_type == "uom":
                        self.uom_codes = resp["object"][set_type]
                    elif set_type == "survey":
                        self.survey_codes = resp["object"][set_type]
                    elif set_type == "subject":
                        self.subject_codes = resp["object"][set_type]
                    elif set_type == "classificationType":
                        self.subject_codes = resp["object"][set_type]
                    elif set_type == "securityLevel":
                        self.security_level_codes = resp["object"][set_type]
                    elif set_type == "terminated":
                        self.terminated_codes = resp["object"][set_type]
                    elif set_type == "wdsResponseStatus":
                        self.wds_response_status_codes = resp["object"][set_type]
                retval = True
        return retval

    def get_cube_metadata(self, product_id):
        # submits WDS request, returns cube metadata for 8 digit product_id
        url = self.wds_url + "getCubeMetadata"
        post_vars = [{"productId": int(product_id)}]
        log.info("Retrieving " + str(product_id) + " metadata from " + url)
        r = requests.post(url, json=post_vars)
        self.check_http_request_status(r)

        retval = False
        if self.last_http_req_status:
            resp = r.json()

            if resp[0]["status"] != "SUCCESS":
                log.warning("Cube metadata could not be retrieved. WDS returned: " + str(resp["status"]) +
                            " for product " + str(product_id))
            else:
                retval = resp[0]["object"]

        return retval

    def get_delta_file(self, rel_date, file_path):
        # download delta file for relase date(rel_date) and save to file_path
        delta_link = self.delta_url + str(rel_date) + ".zip"
        log.info("Downloading Delta File: " + delta_link)
        dl_d = requests.get(delta_link)
        self.check_http_request_status(dl_d)

        retval = False
        if self.last_http_req_status:
            if write_file(file_path, dl_d.content, "wb"):
                retval = True
        else:
            log.warning("Delta file could not be downloaded.")
        return retval

    def get_full_table_download(self, product_id, lang_code, file_path):
        # submits WDS request, saves full table as zipped csv
        # product_id - 8 digit product id
        # lang_code - language code "en" or "fr"
        # file_path - location to save the file
        url = self.wds_url + "getFullTableDownloadCSV/" + str(product_id) + "/" + lang_code
        log.info("Retrieving download link from " + url)
        r = requests.get(url)
        self.check_http_request_status(r)

        retval = False
        if self.last_http_req_status:
            resp = r.json()

            if resp["status"] != "SUCCESS":
                log.warning("Download link could not be retrieved. WDS returned: " + str(resp["status"]) +
                            " for product " + str(product_id) + " " + lang_code)
            else:
                log.info("Downloading file from " + str(resp["object"]) + " " + str(datetime.now()))
                dl_r = requests.get(resp["object"])  # wds returns a link to the zip file, download it
                self.check_http_request_status(dl_r)
                if self.last_http_req_status:
                    if write_file(file_path, dl_r.content, "wb"):
                        retval = True
                else:
                    log.warning("The file could not be downloaded.")
        return retval
