"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains schemas of whatsapp business management package only
    * Description: All the whatsapp business management routes present here
"""

from flask import Blueprint, jsonify
from flask_cors import cross_origin
from flask_jwt_extended import get_jwt

# Import custom packages
from wacore.pkg_waba_management.mod_waba_management_functions import ClsWabaManagementFunc
from wacore.pkg_extras.mod_common import ClsCommon
from wacore.auth.mod_login_functions import token_required
from walogger.walogger import WaLogger

app_waba_management = Blueprint('app_waba_management', __name__,url_prefix='/waapi/waba_management')

# Initialize logger with name that we want or any other
obj_log = WaLogger('pkwbhk')
lg = obj_log.get_logger()


class ClsWabaManagement():
    """" Class called to perform function regarding to whatsapp business management"""

    def __init__(self):
        """ Create or initialize object and variables """
        pass

    @app_waba_management.route("/connected_phone_numbers", methods=["GET"])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_get_phone_numbers():
        """ Method called to get connected phone numbers """
        claims = get_jwt()
        client_id = claims["ew_id"]
        email_id = claims["email_id"]
        bot_id = claims["bot_id"]
        accessible_phones = claims["accessible_phones"]
        print(f"thisisaccessible_phones{accessible_phones}")
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id: " + str(e))    
            return jsonify({"id": "1031", "message": "Invalid credentials", "description": "Invalid client number", "data": "", "success": False})        
        if "error" in db_client_waba_settings or db_client_waba_settings["response"] == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings : None")    
            return jsonify({"id": "1032", "message": "Invalid credentials", "description": "Invalid client number. Please use correct client number", "data": "", "success": False})
        
        try:
            obj_waba_management = ClsWabaManagementFunc(db_client_waba_settings)
            result = obj_waba_management.func_phone_numbers_info(client_id,accessible_phones,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "connected_phone_numbers API failed: " + str(e))
            return jsonify({"id": "1033", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})
