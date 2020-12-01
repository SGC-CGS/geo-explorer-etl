# WDS class
import requests
from pprint import pprint


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

    def check_http_request_status(self, r)\
            :
        # Verify http request status.
        # note some services will return 404 if there is no data available, this doesn't handle that yet.
        if r.status_code == requests.codes.ok:
            self.last_http_req_status = True
        else:
            print("Could not access WDS because of error: " + str(r.status_code))
            print("See detailed error message below.\n")
            r.raise_for_status()
            self.last_http_req_status = False
        return

    def check_wds_response_status_code(self, response_code):
        # wds returns a response code for each line item
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
        print("Accessing " + url + "...")
        r = requests.get(url)
        self.check_http_request_status(r)

        retval = False
        if self.last_http_req_status:
            resp = r.json()
            if resp["status"] != "SUCCESS":
                print("Changed cube list could not be retrieved. WDS returned: "
                      + str(resp["status"]) + " for Date " + str_date)
            else:
                prod_list = []
                for row in resp["object"]:
                    prod_list.append(row["productId"])
                retval = prod_list

        return retval

    def get_code_sets(self):
        # submits WDS request, returns list of code sets
        url = self.wds_url + "getCodeSets"
        print("Retrieving code sets from " + url + "...\n")
        r = requests.get(url)
        self.check_http_request_status(r)

        retval = False
        if self.last_http_req_status:
            resp = r.json()
            if resp["status"] != "SUCCESS":
                print("Code set list could not be retrieved. WDS returned: " + str(resp["status"]))
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
                # print(self.check_wds_response_status_code(8)) # example to look up a code
        return retval

    def get_cube_metadata(self, product_id):
        # submits WDS request, returns cube metadata for product_id
        # product_id - 8 digits
        url = self.wds_url + "getCubeMetadata"
        post_vars = [{"productId": int(product_id)}]
        print("Retrieving " + str(product_id) + " metadata from " + url + "...\n")
        r = requests.post(url, json=post_vars)
        self.check_http_request_status(r)

        retval = False
        if self.last_http_req_status:
            resp = r.json()

            if resp[0]["status"] != "SUCCESS":
                print("Cube metadata could not be retrieved. WDS returned: "
                      + str(resp["status"]) + " for product " + str(product_id))
            else:
                retval = resp[0]["object"]

        return retval

    def get_delta_file(self, rel_date, file_path):
        # download delta file for relase date(rel_date) and save to file_path
        delta_link = self.delta_url + str(rel_date) + ".zip"
        print("Downloading Delta File: " + delta_link)
        dl_d = requests.get(delta_link)
        self.check_http_request_status(dl_d)

        retval = False
        if self.last_http_req_status:
            try:
                with open(file_path, "wb") as f:
                    f.write(dl_d.content)  # save the file
            except IOError as e:
                print("Failed saving to " + file_path)
                print("Error: File could not be written to disk. \n" + str(e))
            else:
                print("File saved to " + file_path)
                retval = True
        else:
            print("Delta file could not be downloaded.")
        return retval

    def get_full_table_download(self, product_id, lang_code, file_path):
        # submits WDS request, saves full table as zipped csv
        # product_id - 8 digit product id
        # lang_code - language code "en" or "fr"
        # file_path - location to save the file
        url = self.wds_url + "getFullTableDownloadCSV/" + str(product_id) + "/" + lang_code
        print("Retrieving CSV link from " + url + "...\n")
        r = requests.get(url)
        self.check_http_request_status(r)

        retval = False
        if self.last_http_req_status:
            resp = r.json()

            if resp["status"] != "SUCCESS":
                print("CSV file link could not be retrieved. WDS returned: " + str(resp["status"])
                      + " for product " + str(product_id) + " " + lang_code)
            else:
                print("Downloading CSV file from " + str(resp["object"]))
                dl_r = requests.get(resp["object"])  # wds returns a link to the zip file
                self.check_http_request_status(dl_r)
                if self.last_http_req_status:
                    print("Saving file as zip.")
                    try:
                        with open(file_path, "wb") as f:
                            f.write(dl_r.content)  # save the file
                    except IOError as e:
                        print("Error: File could not be written to disk. \n" + str(e))
                    else:
                        print("File saved.")
                        retval = True
                else:
                    print("CSV file could not be downloaded.")
        return retval

    def get_members_from_dimension(self, dimension):
        retval = False
        if dimension["member"]:
            retval = dimension["member"]
            print("MEMBER")
            pprint(retval)
        return retval
