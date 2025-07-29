import json
import requests
from flask import jsonify

#import custom packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from ..global_variable import graph_url


class ClsDeepLink():
    """ Class called to perform deep link url related actions """

    def __init__(self):
        """
        Initialize required object and variables:
        Local Variable:
            client_db_name(str): Fetch and generate DB name for individual client
            ew_db(DB object): Initialize DB
            client_db(DB Object):  Initialize DB
            contact_collection_name (collection Object name): Create collection name

        Returns:
            All above parameteres and variables
        """
        self.ew_db = ClsMongoDBInit.get_ew_db_client()

    
    def func_create_deep_link_url(self,data1,lg):
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"cc_client_number": data1["client_number"]}, {"_id":0})
            db_wa_system_account = self.ew_db.wa_system_account.find_one({})
        except Exception as e:
            lg.error("DB error - client_waba_settings : " + str(e))
            return jsonify({"id": "1302", "message": "Data Query Error", "description": "", "data": "", "success": False})
        if db_client_waba_settings == None:
            lg.error("DB error - client_waba_settings : None")
            return jsonify({"id": "1303", "message": "Invalid user input", "description": "Invalid client id or client number", "data": "", "success": False})

        str_waba_id = db_client_waba_settings.get("waba_id")

        phone_numbers_url = graph_url + str_waba_id + "/phone_numbers" + "?access_token=" + db_wa_system_account["system_user_token"]
        phone_numbers_response = requests.request("GET", phone_numbers_url, headers={}, data={})
        
        if "error" not in phone_numbers_response.json().keys():
            json_phone_number = phone_numbers_response.json()
        else:
            return jsonify({"id": "1304", "message": "Error in phone number api response", "description": "Invalid client number", "data": "", "success": False})

        lst_status_id = []
        for number_data in json_phone_number.get("data"):
            if data1["client_number"] == number_data.get("display_phone_number").replace(" ", "")[1:]:
                lst_status_id.append(number_data.get("id"))
        
        if lst_status_id == []:
            return jsonify({"id": "1305", "message": "Client number does not exists", "description": "Invalid client number", "data": "", "success": False})
        else:
            str_current_status_id = lst_status_id[0]
        
        gen_deep_link_url = graph_url + str_current_status_id + "/message_qrdls" + "?access_token=" + db_wa_system_account["system_user_token"]
        gen_deep_link_payload = json.dumps({"prefilled_message": "Hi"})
        gen_deep_link_headers = {'Content-Type': 'application/json'}
        gen_deep_link_response = requests.request("POST", gen_deep_link_url, headers=gen_deep_link_headers, data=gen_deep_link_payload)

        if "error" not in phone_numbers_response.json().keys():
            json_gen_deep_link = gen_deep_link_response.json()
        else:
            return jsonify({"id": "1306", "message": "Error in get deep link url api response", "description": "Invalid client number", "data": "", "success": False})

        gen_qr_url = graph_url + str_current_status_id + "/message_qrdls/" + json_gen_deep_link.get("code") + "?generate_qr_image=png&access_token=" + db_wa_system_account["system_user_token"]
        gen_qr_payload = json.dumps({"prefilled_message": "Hi"})
        gen_qr_headers = {'Content-Type': 'application/json'}

        gen_qr_response = requests.request("POST", gen_qr_url, headers=gen_qr_headers, data=gen_qr_payload)
        if "error" not in gen_qr_response.json().keys():
            try:
                self.ew_db.client_profile.update_one({"client_number": data1["client_number"]},{"$set": {"deep_link_url": gen_qr_response.json().get("deep_link_url"), "qr_code_url": gen_qr_response.json().get("qr_image_url"), "qr_code_id": gen_qr_response.json()["code"], "phone_number_id": str_current_status_id}})
                return jsonify({"data": {"deep_link_url" : gen_qr_response.json().get("deep_link_url"), "qr_code_url": gen_qr_response.json().get("qr_image_url")}, "description": "", "id": "1023", "message": "Deep link url and Whatsapp QR code url created successfully", "success": True})
            except Exception as e:
                lg.error("DB error - client_waba_settings : " + str(e))
                return jsonify({"id": "1307", "message": "Data Query Error", "description": "", "data": "", "success": False})
        else:
            return jsonify({"id": "1308", "message": "Error in get qr code url api response", "description": "Invalid client number", "data": "", "success": False})
    

    def func_get_deep_link_url(self,data,lg):
        try:
            db_client_profile = self.ew_db.client_profile.find_one({"client_number": data["client_number"]}, {"_id":0})
        except Exception as e:
            lg.error("DB error - client_waba_settings : " + str(e))
            return jsonify({"id": "1342", "message": "Data Query Error", "description": "", "data": "", "success": False})

        if db_client_profile == None:
            lg.error("DB error - client_waba_settings : None")
            return jsonify({"id": "1343", "message": "Invalid user input", "description": "Invalid client id or client number", "data": "", "success": False})

        if "deep_link_url" not in db_client_profile:
            deep_link_url = ""
        else:    
            deep_link_url = db_client_profile["deep_link_url"]

        if "qr_code_url" not in db_client_profile:
            qr_code_url = ""
        else:
            qr_code_url = db_client_profile["qr_code_url"]

        return jsonify({"data": {"deep_link_url" : deep_link_url, "qr_code_url": qr_code_url}, "description": "", "id": "1344", "message": "Deep link url and Whatsapp QR code url fetched successfully", "success": True})


    def func_delete_deep_link_url(self,data,lg):
        try:
            db_client_profile = self.ew_db.client_profile.find_one({"client_number": data["client_number"]}, {"_id":0})
            db_wa_system_account = self.ew_db.wa_system_account.find_one({})
        except Exception as e:
            lg.error("DB error - client_waba_settings : " + str(e))
            return jsonify({"id": "1352", "message": "Data Query Error", "description": "", "data": "", "success": False})

        if db_client_profile == None:
            lg.error("DB error - client_waba_settings : None")
            return jsonify({"id": "1353", "message": "Invalid user input", "description": "Invalid client id or client number", "data": "", "success": False})
        
        url = graph_url + db_client_profile["phone_number_id"] + "/message_qrdls/" + db_client_profile["qr_code_id"] + "?access_token=" + db_wa_system_account["system_user_token"]
        response = requests.request("DELETE", url, headers={}, data={})
        
        if "error" not in response.json():
            self.ew_db.client_profile.update_one({"client_number": data["client_number"]}, {"$set": {"deep_link_url": "", "qr_code_url": "", "phone_number_id": "", "qr_code_id": ""}})
        else:
            return jsonify({"data": "", "description": "", "id": "1355", "message": "Unable to delete Deep link url and Whatsapp QR code url", "success": False})

        return jsonify({"data": "", "description": "", "id": "1356", "message": "Deep link url and Whatsapp QR code url deleted successfully", "success": True})
