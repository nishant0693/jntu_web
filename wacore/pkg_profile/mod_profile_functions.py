from flask.json import jsonify
import requests
import json
import os
import logging
import logging.config

# Import custom packages
from ..pkg_extras.mod_common import ClsCommon
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from ..auth.mod_login_functions import phone_access_required

# Calling class objects
obj_common = ClsCommon()



class ClsProfile():
    """ Class called to perform profile actions """

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
  

    @phone_access_required    
    def func_set_complete_profile(self,str_client_id,str_client_number,data,lg):
        """ Method called to set complete profile """
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({'ew_id': str_client_id, 'cc_client_number': str_client_number},{"_id":0})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings : " + str(e))    
            return jsonify({"id": "1002", "message": "Data Query Error", "description": "", "data": "", "success": False}) 
        
        if db_client_waba_settings == None:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings : " + "None")
            return jsonify({"id": "1003", "message": "Invalid user input", "description": "Invalid client id and client number", "data": "", "success": False}) 
        
        try:
            str_token = obj_common.func_get_access_token(db_client_waba_settings)
            var_about_url = str(db_client_waba_settings['url']) + "settings/profile/about"
            var_about_payload= json.dumps({'text': data['about']})
            var_about_headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + str(str_token)}
            var_about_response = requests.request("PATCH", var_about_url, headers=var_about_headers, data=var_about_payload, verify=False)
            var_info_url = str(db_client_waba_settings['url']) + "settings/business/profile"
            var_info_response = requests.request("POST", var_info_url, headers=var_about_headers, data=json.dumps({'address': data['address'], 'description': data['description'], 'email': data['email'], 'vertical': data['vertical'], 'websites': data['websites']}), verify=False)            
        except Exception as e: 
            lg.critical("ew_id=" + str(str_client_id) + " | " + "Error in key response : " + str(e))
            return jsonify({"id": "1004", "message": "Invalid key input", "description": "Something went wrong in key", "data": "", "success": False}) 

        if 'errors' not in var_info_response.json().keys():
            try:
                self.ew_db.client_profile.update_one({'client_id': str_client_id, 'client_number': str_client_number}, {'$set': {'about': data['about'], 'address': data['address'], 'description': data['description'], 'email': data['email'], 'vertical': data['vertical'], 'websites': data['websites']}})
                return jsonify({"id": "1005", "message": "Profile set successfully", "description": "", "data": "", "success": True})
            except Exception as e:
                lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_profile : " + str(e))    
                return jsonify({"id": "1006", "message": "Data Query Error", "description": "", "data": "", "success": False})
        else:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "Error in WA API update business profile : " + str(var_about_response.json()))
            return jsonify({"id": "1007", "message": "Failed to set profile", "description": var_info_response.json(), "data": "", "success": True})


    @phone_access_required
    def func_get_complete_profile(self,str_client_id,str_client_number,lg):
        """ Method called to get complete profile """

        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({'ew_id': str_client_id, 'cc_client_number': str_client_number},{'_id':0})
        except Exception as e:
            lg.error("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings : " + str(e))
            return jsonify({"id": "1012", "message": "Data Query Error", "description": "", "data": "", "success": False})

        if db_client_waba_settings == None:
            lg.error("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings : None")
            return jsonify({"id": "1013", "message": "Invalid user input", "description": "Invalid client id and client number", "data": "", "success": False}) 
        
        try:
            # str_token = obj_common.func_get_access_token(db_client_waba_settings)
            str_token = db_client_waba_settings['access_token']
            var_headers_data = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + str(str_token) }
            var_response_profile = requests.request('GET', str(db_client_waba_settings['url'])+str(db_client_waba_settings['Phone-Number-ID']) + '/'+'whatsapp_business_profile?fields=about,address,description,email,profile_picture_url,websites', headers=var_headers_data, data={}, verify=False)
            lg.info(f"var_response_Profile value is {var_response_profile.text}")
            if 'data' in var_response_profile.json():
                str_text = var_response_profile.json()
                str_profile = str_text['data'][0]
                lg.info(f"str_profile value is  {str_profile}")
                if 'profile_picture_url' in str_profile:
                    lg.info(f"inside if condition of profile check")
                    str_profile = str_text['data'][0]["profile_picture_url"]
                else:
                    lg.info(f"inside else condition of profile check")
                    str_profile = " "


            else:
                str_profile = ""
            self.ew_db.client_profile.update_one({"client_id": str_client_id, "client_number": str_client_number}, {"$set": {"profile_picture": str_profile}})
            db_data = self.ew_db.client_profile.find_one({'client_id': str_client_id, 'client_number': str_client_number}, {'_id':0})
            return jsonify({"id":"1014", "message": "Profile fetched successfully", "description": "", "data": db_data, "success": True})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "Error in key response : " + str(e))
            return jsonify({"id": "1015", "message": "Invalid key input", "description": "Something went wrong with key or WA GET reuqest failed", "data": "", "success": False}) 


    @phone_access_required
    def func_set_profile_picture(self,str_client_id,str_client_number,obj_imagefile,lg):
        """ Method called to set profile photo """
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"ew_id": str_client_id, "cc_client_number": str_client_number},{"_id":0})        
        except Exception as e: 
            lg.error("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings : " + str(e))
            return jsonify({"id": "1022", "message": "Data Query Error", "description": "", "data": "", "success": False})
        if db_client_waba_settings == None:
            lg.error("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings : None")
            return jsonify({"id": "1023", "message": "Invalid user input", "description": "Invalid client id or client number", "data": "", "success": False})
        try:
            str_token = obj_common.func_get_access_token(db_client_waba_settings)
            var_url = str(db_client_waba_settings['url']) + "settings/profile/photo"
            if obj_imagefile.filename.lower().endswith(('.png', '.jpg')) and os.fstat(obj_imagefile.fileno()).st_size < 5242880:
                var_payload = obj_imagefile
                var_headers = {'Content-Type': 'image/jpeg','Authorization': 'Bearer ' + str(str_token)}
                var_response = requests.request("POST", var_url, headers=var_headers, data=var_payload, verify=False)
                if 'errors' in var_response.json():
                    lg.info("ew_id=" + str(str_client_id) + " | " + "WA API response for set profile picture: " + str(var_response.json()))
                    return jsonify({"error": {"id": "1024", "message": "Please, upload appropriate image"}, "success": False})
                else:
                    url1 = str(db_client_waba_settings['url']) + "settings/profile/photo?format=link"
                    var_response1 = requests.request("GET", url1, headers=var_headers, data={}, verify=False)
                    var_resp = var_response1.json()
                    var_profile_url = var_resp['settings']['profile']['photo']['link']
                    self.ew_db.client_profile.update_one({"ew_id": str_client_id, "client_number": str_client_number}, {"$set": {"profile_picture": var_profile_url}})
                return jsonify({"id": "1025", "message": "Profile picture set successfully", "description": "", "data": "", "success": True})
            else:
                lg.critical("ew_id=" + str(str_client_id) + " | " "Invalid image format. Upload image either in .png or .jpg format and size less than 5200000 bytes")
                return jsonify({"id": "1026", "message": "Invalid image format or size", "description": "Please, upload image either in .png or .jpg format and size less than 5200000 bytes", "data": "", "success": False})

        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "Error in key response : " + str(e))
            return jsonify({"id": "1027", "message": "Invalid key input", "description": "Something went wrong with key or request failed", "data": "", "success": False})