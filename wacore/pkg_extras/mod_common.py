"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains common only
    * Description: All the common functions used all over the system present here  
"""

import requests
from datetime import datetime
from requests.auth import HTTPBasicAuth
from flask import jsonify

# Import custom packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from walogger.walogger import WaLogger

# Initialize logger with name that we want or any other
obj_log = WaLogger('pkcommon')
lg = obj_log.get_logger()

class ClsCommon():
    """ Class for Retriving all common functions"""

    def __init__(self):
        """ Create or initialize object and variables """
        self.ew_db = ClsMongoDBInit.get_ew_db_client()


    def func_add_broadcast_log(self,cl_db,broadcast_id,log_data):
        """ Create broadcast document and add/update stepwise logs """
        cl_db.broadcast_details.update_one({"broadcast_id": broadcast_id},{"$push":{"broadcast_logs":log_data}},upsert=True)
        return True


    def func_get_access_token(self,dict_client):
        """ Retuns the token for the client """
        str_token1 = dict_client['access_token']
        str_expiry = dict_client['token_expires_after']
        str_datetime = datetime.strptime(str_expiry[:-6], '%Y-%m-%d %H:%M:%S')
        str_current_datetime = datetime.utcnow()
        if str_datetime > str_current_datetime:
            str_token = str_token1
        else:
            var_url = dict_client['url'] + 'users/login'
            var_payload={}
            var_headers = {'Content-Type': 'application/json'}
            var_response = requests.request("POST", var_url, headers=var_headers, data=var_payload, auth=HTTPBasicAuth(dict_client['username'], dict_client['password']),verify=False)
            var_users = var_response.json()
            for str_user in var_users['users']:
                str_token = str_user['token']
                str_expiry = str_user['expires_after']
                try:
                    db_client_waba_settings = self.ew_db.client_waba_settings.update_one({"ew_id": dict_client["ew_id"], "client_number": dict_client["client_number"]},{"$set": {"access_token": str_token, "token_expires_after": str_expiry}})    
                except Exception as e:
                    lg.critical("DB error - client_waba_settings : " + str(e))
                    return jsonify({"error": {"id": "1261", "message": "Data Query Error"}, "success": False})
        return str_token


    def get_waba_settings(self,client_number):
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"client_number": client_number}, {"_id": 0})
            return {"response": db_client_waba_settings}
        except Exception as e:
            lg.critical("DB error - client_waba_settings : " + str(e))
            return jsonify({"error": {"id": "1262", "message": "Data Query Error"}, "success": False})


    def get_waba_settings_by_bot_id(self,bot_id):
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"bot_id": bot_id}, {"_id": 0})
            return {"response": db_client_waba_settings}
        except Exception as e:
            lg.critical("DB error - client_waba_settings : " + str(e))
            return jsonify({"error": {"id": "1263", "message": "Data Query Error"}, "success": False})


    def get_waba_settings_by_cc_client_number(self,client_number):
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"cc_client_number": client_number}, {"_id": 0})
            return {"response": db_client_waba_settings}
        except Exception as e:
            lg.critical("DB error - client_waba_settings : " + str(e))
            return jsonify({"error": {"id": "1264", "message": "Data Query Error"}, "success": False})


    def get_waba_settings_by_waba_id(self,waba_id):
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"waba_id": waba_id}, {"_id": 0})
            return {"response": db_client_waba_settings}
        except Exception as e:
            lg.critical("DB error - client_waba_settings : " + str(e))
            return jsonify({"error": {"id": "1264_1", "message": "Data Query Error"}, "success": False})


    def get_waba_settings_by_cc_client_number_bot_id(self,client_number,bot_id):
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"cc_client_number": client_number,"bot_id": bot_id}, {"_id": 0})
            return {"response": db_client_waba_settings}
        except Exception as e:
            lg.critical("DB error - client_waba_settings : " + str(e))
            return jsonify({"error": {"id": "1265", "message": "Data Query Error"}, "success": False})
    

    def __del__(self):
        self.ew_db.client.close()