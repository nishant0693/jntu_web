"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains schemas of profile package only
    * Description: All the profile routes are present here
"""

from flask import Blueprint, jsonify, request
from flask_cors import cross_origin
from flask_jwt_extended import get_jwt

# Import custom packages
from wacore.pkg_profile.mod_profile_functions import ClsProfile
from wacore.pkg_extras.mod_common import ClsCommon
from wacore.auth.mod_login_functions import token_required
from walogger.walogger import WaLogger
from wacore.pkg_profile.mod_deep_link_functions import ClsDeepLink

app_profile = Blueprint('app_profile', __name__,url_prefix='/waapi/profile')  

# Initialize logger with name that we want or any other
obj_log = WaLogger('pkProfile')
lg = obj_log.get_logger()

class ProfileRoutes():
    """ Class called for template routes"""

    def __init__(self):
        """ Create or initialize object and variables """
        pass


    @app_profile.route("/set_profile", methods=["POST"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_set_profile():
        """ Method called to set profile """
        # client_number = request.args.get("client_number")
        # claims = get_jwt()
        # client_id = claims["ew_id"]
        # data = request.json
        # try:
        #     obj_profile = ClsProfile()
        #     result = obj_profile.func_set_complete_profile(client_id,client_number,data,lg)
        #     return result
        # except Exception as e:
        #     lg.critical("ew_id=" + str(client_id) + " | " + "set_profile API failed : " + str(e))
        #     return jsonify({"id": "1001", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})
        return jsonify({"id": "1005", "message": "Profile set successfully", "description": "", "data": "", "success": True})


    @app_profile.route("/get_profile", methods=["GET"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_get_profile():
        """ Method called to get profile """
        client_number = request.args.get("client_number")
        claims = get_jwt()
        client_id = claims["ew_id"]
        try:
            obj_profile = ClsProfile()
            result = obj_profile.func_get_complete_profile(client_id,client_number,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "get_profile API failed: " + str(e))
            return jsonify({"id": "1011", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_profile.route("/set_profile_photo", methods=["POST"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_set_profile_photo():
        """ Method called to set profile photo """
        # client_number = request.args.get("client_number")
        # claims = get_jwt()
        # client_id = claims["ew_id"]
        # imagefile = request.files['image']
        # try:
        #     obj_profile = ClsProfile()
        #     result = obj_profile.func_set_profile_picture(client_id,client_number,imagefile,lg)
        #     return result
        # except Exception as e:
        #     lg.critical("ew_id=" + str(client_id) + " | " + "set_profile_photo API failed: " + str(e))
        #     return jsonify({"id": "1021", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})
        return jsonify({"id": "1025", "message": "Profile picture set successfully", "description": "", "data": "", "success": True})


    @app_profile.route("/create_deep_link_url", methods=["POST"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_deep_link():
        """ Method called to create deep link url """
        claims = get_jwt()
        client_id = claims["ew_id"]
        data = request.json
        try:
            obj_profile = ClsDeepLink()
            result = obj_profile.func_create_deep_link_url(data,lg)
            return result
        except Exception as e:
            lg.critical("deep_link_url API failed: " + str(e))
            return jsonify({"id": "1301", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})

    
    @app_profile.route("/get_deep_link_url", methods=["POST"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_deep_link_get():
        """ Method called to get deep link url """
        claims = get_jwt()
        client_id = claims["ew_id"]
        data = request.json
        try:
            obj_profile = ClsDeepLink()
            result = obj_profile.func_get_deep_link_url(data,lg)
            return result
        except Exception as e:
            lg.critical("deep_link_url API failed: " + str(e))
            return jsonify({"id": "1341", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})

        
    @app_profile.route("/delete_deep_link_url", methods=["POST"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_deep_link_delete():
        """ Method called to delete deep link url """
        claims = get_jwt()
        client_id = claims["ew_id"]
        data = request.json
        try:
            obj_profile = ClsDeepLink()
            result = obj_profile.func_delete_deep_link_url(data,lg)
            return result
        except Exception as e:
            lg.critical("deep_link_url API failed: " + str(e))
            return jsonify({"id": "1351", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})