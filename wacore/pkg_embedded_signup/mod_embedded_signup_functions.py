"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains schemas of DMP embedded signup package only
    * Description: All the Embedded signup functions present here
"""

from flask import jsonify
import requests
import datetime
import pymongo

# Import custom packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from ..global_variable import graph_url, channel_dtls_db, connection_string


class ClsSignup():
    """ Class for Embedded Signup functions"""

    def __init__(self):
        """ Create or initialize object and variables """
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        self.client = pymongo.MongoClient(connection_string)
        self.channelDetails = self.client[channel_dtls_db]
        pass


    def func_check_bmid_status(self,str_bmid,lg):
        """ Function called to check BMID status """
        try:
            db_business_info_data = self.ew_db.client_business_info.find_one({"BMID": str_bmid}, {"_id": 0, "BMID": 1, "bot_id": 1, "completed_business_step": 1})
        except Exception as e:
            lg.critical("DB error - client_business_info : " + str(e))
            return jsonify({"id": "2132", "msg": "Data Query Error", "success": False})
        if db_business_info_data == None:
            return jsonify({"data": {"BMID": str_bmid, "completed_business_step": "step0"}, "success": True})
        else:            
            return jsonify({"data": db_business_info_data, "success": True})

    
    def func_get_bmid(self,str_bot_id,lg):
        """ Function called to get bmid status """
        try:
            db_business_info_data = self.ew_db.client_business_info.find_one({"bot_id": str_bot_id}, {"_id": 0, "BMID": 1})
        except Exception as e:
            lg.critical("DB error - client_business_info : " + str(e))
            return jsonify({"id": "2142", "msg": "Data Query Error", "success": False})

        if db_business_info_data == None:
            lg.critical("DB error - client_business_info : None")
            return jsonify({"msg": "No BMID is mapped with this bot id", "success": False})
        else:
            return jsonify({"response": {"BMID": db_business_info_data["BMID"]}, "success": True})


    def func_store_profile_details(self,str_bot_id,str_bmid,str_business_name,str_business_hq,str_business_address,list_business_website,str_business_description,str_business_email,str_business_phone_number,str_business_verical,str_wa_phone_numbers,lg):
        """ Function called to add profile details of client into DB """
        list_websites = list_business_website.split(",")
        try:
            self.ew_db.client_business_info.insert_one({"BMID": str_bmid, "business_name": str_business_name, "business_headquarter": str_business_hq, "business_address": str_business_address, "business_websites": list_websites, "business_description": str_business_description, "business_email": str_business_email, "business_phone_number": str_business_phone_number, "business_vertical": str_business_verical, "completed_business_step": "step1", "timestamp": int(datetime.datetime.now().timestamp()), "bot_id": str_bot_id, "wa_phone_numbers": []})
        except Exception as e:
            lg.critical("DB error - client_business_info : " + str(e))
            return jsonify({"id": "2152", "msg": "Data Query Error", "success": False})

        return jsonify({"data": {"BMID": str_bmid, "completed_business_step": "step1"}, "msg": "Data added successfully", "success": True})

    
    def func_get_verticals_list(self):
        """ Function called to get vertical list """
        list_verticals_list =["Automotive", "Beauty, Spa and Salon", "Clothing and Apparel", "Education", "Entertainment", "Event Planning and Service", "Finance and Banking", "Food and Grocery", "Public Service", "Hotel and Lodging", "Medical and Health", "Non-profit", "Professional Services", "Shopping and Retail", "Travel and Transportation", "Restaurant", "Other"]
        return list_verticals_list

    
    def func_get_debug_token(self,str_oauth_user_token,str_bmid,lg):
        """ Function called to get debug token """
        try:
            self.ew_db.debug_tokens.insert({"debug_token": str_oauth_user_token, "BMID": str_bmid})
            db_wa_account = self.ew_db.wa_system_account.find_one({},{"_id": 0})
        except Exception as e:
            lg.critical("DB error - debug_tokens, wa_system_account : " + str(e))
            return jsonify({"id": "2172", "msg": "Data Query Error", "success": False})

        str_system_user_token = db_wa_account["system_user_token"]
        str_url = graph_url + "debug_token?input_token=" + str_oauth_user_token + "&access_token=" + str_system_user_token
        # url = https://graph.facebook.com/v16.0/debug_token?input_token = 123&access_token = s123
        # url = https://graph.facebook.com/v16.0/waba_id123/message_templates?name=temp1&access_token=123456

        dict_payload={}
        dict_headers = {}
        response = requests.request("GET", str_url, headers=dict_headers, data=dict_payload)
        dict_resp = response.json()
        try:
            for i in range(len(dict_resp["data"]["granular_scopes"])):
                if "scope" in dict_resp["data"]["granular_scopes"][i].keys():
                    if dict_resp["data"]["granular_scopes"][i]["scope"] == "whatsapp_business_management":
                        int_waba_id = dict_resp["data"]["granular_scopes"][i]["target_ids"][0]
                              
            self.ew_db.debug_tokens.update_one({"BMID": str_bmid}, {"$set": {"waba_id": int_waba_id, "payload": dict_resp}})
        except Exception as e:
            lg.critical("Error in key response : " + str(e))
            return jsonify({"msg": "Key Not Present", "success": False})
       
        if 'error' in response.json()["data"]:
            return jsonify({"msg": response.json()["data"]["error"]["msg"], "success": False})
        else:
            try:
               self.ew_db.client_business_info.update_one({"BMID": str_bmid}, {"$set": {"completed_business_step": "step2"}})
            except Exception as e:
                lg.critical("DB error - client_business_info : " + str(e))
                return jsonify({"id": "2173", "msg": "Data Query Error", "success": False})

            return jsonify({"data": {"BMID": str_bmid, "completed_business_step": "step2"}, "msg": "SignUp completed", "success": True})
    

    def func_get_interface_data(self,str_bot_id,lg):
        """ Function called to get interface data of client"""
        try:
            db_business_info_data = self.ew_db.client_business_info.find_one({"bot_id": str_bot_id}, {"_id": 0, "BMID": 1, "wa_phone_numbers": 1})
        except Exception as e:
            lg.critical("DB error - client_business_info : " + str(e))
            return jsonify({"id": "2182", "msg": "Data Query Error", "success": False})

        if db_business_info_data == None or "BMID" not in db_business_info_data:
            return jsonify({"msg": "Bot id is not mapped with any BMID", "success": False})
        str_bmid = db_business_info_data["BMID"]
        list_client_details = db_business_info_data["wa_phone_numbers"]
        
        try:
            db_debug_token_data = self.ew_db.debug_tokens.find_one({"BMID": str_bmid}, {"_id": 0, "waba_id": 1})
        except Exception as e:
            lg.critical("DB error - debug_tokens : " + str(e))
            return jsonify({"id": "2183", "msg": "Data Query Error", "success": False})

        if db_debug_token_data == None or "waba_id" not in db_debug_token_data:
            lg.critical("DB error - debug_tokens : None")
            return jsonify({"msg": "Invalid BMID", "success": False})
        int_waba_id = db_debug_token_data["waba_id"]

        return jsonify({"response": {"BMID": str_bmid, "waba_id": int_waba_id, "phone_numbers": list_client_details}, "success": True})


    def func_validate_phone_numbers(self,int_phone_number,lg):
        """ Function called to validate phone number """
        try:
            db_interface_details_data = self.channelDetails.whatsapp.find_one({"fields.dest_number": int_phone_number},{"_id": 0})
        except Exception as e:
            lg.critical("DB error - channel_details-whatsapp : " + str(e))
            return jsonify({"id": "2192", "msg": "Data Query Error", "success": False})

        if db_interface_details_data == None:
            return jsonify({"response": {"number_used": False}, "success": True})
        else:
            return jsonify({"response": {"number_used": True}, "success": True})


    def func_refresh_phone_numbers_list(self,str_bot_id,lg):
        """ Function called to refresh phone number list """
        try:
            db_business_info_data = self.ew_db.client_business_info.find_one({"bot_id": str_bot_id}, {"_id": 0, "BMID": 1, "wa_phone_numbers": 1})
        except Exception as e:
            lg.critical("DB error - client_business_info : " + str(e))
            return jsonify({"id": "2202", "msg": "Data Query Error", "success": False})

        if db_business_info_data == None or "BMID" not in db_business_info_data:
            lg.critical("DB error - client_business_info : None")
            return jsonify({"msg": "Bot id is not mapped with any BMID", "success": False})
        str_bmid = db_business_info_data["BMID"]
        
        try:
            db_debug_token_data = self.ew_db.debug_tokens.find_one({"BMID": str_bmid}, {"_id": 0, "waba_id": 1})
            if db_debug_token_data == None or "waba_id" not in db_debug_token_data:
                lg.critical("DB error - debug_tokens : None")
                return jsonify({"msg": "Invalid BMID", "success": False})
            int_waba_id = db_debug_token_data["waba_id"]
            db_system_user_token_data = self.ew_db.wa_system_account.find_one({},{"_id": 0, "system_user_token": 1})
        except Exception as e:
            lg.critical("DB error - debug_tokens, wa_system_account : " + str(e))
            return jsonify({"id": "2203", "msg": "Data Query Error", "success": False})

        url = graph_url + int_waba_id + "/phone_numbers?access_token=" + db_system_user_token_data["system_user_token"]
        response = requests.request("GET", url, headers= {}, data= {})
        
        if 'error' not in response.json():
            list_data = []
            for var_single_data in response.json()["data"]:
                list_display_number_data = var_single_data["display_phone_number"].split(" ")
                dict_single_number_details = {"country_code": list_display_number_data[0], "wa_number": list_display_number_data[1] + list_display_number_data[2], "wa_display_name": var_single_data["verified_name"]}
                list_data.append(dict_single_number_details)
            
            list_unmatched_data = []
            for i in list_data:
                if i not in db_business_info_data["wa_phone_numbers"]:
                    list_unmatched_data.append(i)

            var_new_client_details = db_business_info_data["wa_phone_numbers"] + list_unmatched_data
            try:
                self.ew_db.client_business_info.update_one({"bot_id": str_bot_id}, {"$set": {"wa_phone_numbers": var_new_client_details}})
            except Exception as e:
                lg.critical("DB error - client_business_info : " + str(e))
                return jsonify({"id": "2204", "msg": "Data Query Error", "success": False})
            
            return jsonify({"response": "Data updated successfully", "success": True})
        else:
            return jsonify({"response": "Waba id found for bot id is not valid", "success": False})