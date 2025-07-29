"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains schemas of whatsapp business management package only
    * Description: All the whatsapp business management functions present here
"""

from flask import jsonify
import requests

from wacore.auth.mod_login_functions import phone_access_required

# Import custom packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from ..pkg_extras.mod_common import ClsCommon


class ClsWabaManagementFunc():
    """ Class called to for whatsapp business management functions"""
    
    def __init__(self,db_client_waba_settings):
        """ Create or initialize object and variables """
        self.db_client_waba_settings = db_client_waba_settings
        try:
            self.client_db_name = db_client_waba_settings["response"]["ew_id"].lower() + "_" + db_client_waba_settings["response"]["waba_id"]
        except:
            self.client_db_name = ""
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        try:
            self.cl_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)
        except:
            pass    
        pass
    
    
    @phone_access_required
    def func_phone_numbers_info(self,str_client_id,accessible_phones,lg):
        """ Method called to get all phone numbers information of client for dashboard """
        try:
            db_client_business_info = self.ew_db.client_business_info.find_one({"ew_id": str_client_id}, {"_id":0})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_business_info : " + str(e))    
            return jsonify({"id": "1034", "message": "Data Query Error", "description": "", "data": "", "success": False})
        
        if db_client_business_info == None:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_business_info : None")    
            return jsonify({"id":"1035", "message": "Invalid user input", "description": "Template doesnt exist in account", "data": "", "success": False})
            
        if db_client_business_info["wa_phone_numbers"] == []:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_business_info : wa numbers not found")    
            return jsonify({"id":"1036", "message": "Invalid user input", "description": "No number linked with client id", "data": "", "success": False})

        result_data = []
        for contact in db_client_business_info["wa_phone_numbers"]:
            if contact["wa_number"] is not None:
                lg.critical(f"contact {contact}")
                print(f"contact{contact}")
                # accessible_phones= [int(phone['numberLong']) for phone in accessible_phones]
                # print("thisisupdatedaccesibile_nubers{accessible_phones}")
                # accessible_phones=[966592628460]
                if int(contact["country_code"][1:] + contact["wa_number"]) in accessible_phones: 
                    try:
                        db_client_profile = self.ew_db.client_profile.find_one({'client_number': contact["country_code"][1:] + contact["wa_number"]})
                        lg.critical(f"db_client_profileis{db_client_profile}")
                        print(f"db_client_profileis{db_client_profile}")
                        db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"ew_id": str_client_id, "client_number": contact["wa_number"]})
                        # str_token = ClsCommon().func_get_access_token(db_client_waba_settings)
                        # lg.info(f"access_token is {str_token}")
                        db_country_details = self.ew_db.country_details.find_one({},{"_id":0})
                        # print(f"thisisdb_countrydetails{db_country_details}")
                    except Exception as e:
                        lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings, client_profile, country_details : " + str(e))    
                        return jsonify({"id": "1037", "message": "Data Query Error", "description": "", "data": "", "success": False})
                    
                    for country in db_country_details['country']:
                        if country['dial_code'] == contact["country_code"]:
                            str_country_code = country['code']
                            # print(f"insidecountrycodecheck")
                            # lg.critical(f"insidecountrycodecheck")
                    try:
                        lg.critical("dev")
                        str_token = 'EAAjcKG57dJcBO3zuOL9JkZCU90M4T0a6wALuCHqz61P1E8uIiJxZC6h91jwiJUa3cBay9EX8trp4WZBgCmMcZCL9jtx6OetkCyzRQMEZBtnWlXB85ZBaWYAW0dHvwU3ZCMNZAZBPeeNkKz6ZBqEC0z8dExZAckFirt56aXI2ZAN7XEIXrUl2ZCn2XWedBVycJ8euugSNs'
                        # var_health_url = db_client_waba_settings['url'] +db_client_waba_settings['Phone-Number-ID']
                        var_health_url = db_client_waba_settings['url'] +   db_client_waba_settings['waba_id']+'?fields=health_status'
                        # var_health_url = ['Phone-Number-ID']
                        lg.critical(f"Value of var_health_url is {var_health_url}")
                        print(f"Value of var_health_url is {var_health_url}")
                        var_payload = {}
                        var_headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + str(str_token) }
                        var_response_health = requests.request('GET', var_health_url, headers=var_headers, data=var_payload, verify=False)
                        lg.critical(f"value of var_response_health is {var_response_health.text}")
                        print(f"value of var_response_health is {var_response_health.text}")
                        var_resp_health = var_response_health.json()
                    except Exception as e:
                        lg.critical("ew_id=" + str(str_client_id) + " | " + "Error in WA health api : " + str(var_resp_health.json()))    
                        var_resp_health = {}
                        str_status = 'offline'   
                     
                    if "health" in var_resp_health.keys():
                        if "gateway_status" in var_resp_health["health"].keys():
                            if var_resp_health["health"]["gateway_status"] == "connected":
                                str_status = "connected"
                            else:
                                str_status = "connected"
                    
                        elif len(var_resp_health["health"]) > 1:
                            lst_master_health_status = []
                            lst_core_health_status = []
                            for health_key_data in var_resp_health["health"].keys():
                                if health_key_data.startswith("master"):
                                    lst_master_health_status.append(var_resp_health["health"][health_key_data]["gateway_status"])
                                elif health_key_data.startswith("wacore"):
                                    lst_core_health_status.append(var_resp_health["health"][health_key_data]["gateway_status"])
                            if "connected" in lst_master_health_status and "connected" in lst_core_health_status:
                                str_status = "connected"
                            else:
                                str_status = "connected"
                        else:
                            str_status = "connected"
                    else:
                        str_status = "connected"

                    if str_status == 'connected':
                       str_token = db_client_waba_settings['access_token']
                       var_headers_data = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + str(str_token) }
                       var_response_profile = requests.request('GET', str(db_client_waba_settings['url'])+str(db_client_waba_settings['Phone-Number-ID']) + '/'+'whatsapp_business_profile?fields=about,address,description,email,profile_picture_url,websites', headers=var_headers_data, data={}, verify=False)
                       lg.info(f"var_response_Profile value is {var_response_profile.text}")
                       if 'data' in var_response_profile.json():
                            str_text = var_response_profile.json()
                            # str_profile = str_text['data'][0]["profile_picture_url"]
                            str_profile = str_text['data'][0]
                            lg.info(f"str_profile value is  {str_profile}")
                            if 'profile_picture_url' in str_profile:
                                lg.info(f"inside if condition of profile check")
                                str_profile = str_text['data'][0]["profile_picture_url"]
                            else:
                                lg.info(f"inside else condition of profile check")
                                str_profile = " "
                            dict_contact = {"verified_name": contact["wa_display_name"], "phone_number": contact["wa_number"], "country_code": str_country_code, "country_code_number": contact["country_code"][1:], "quality_rating": db_client_profile["quality_rating"], "status": str_status, "profile_picture": str_profile, "messaging_status": db_client_profile["messaging_status"], "message_tier": db_client_profile["message_tier"], "wa_uid": db_client_waba_settings["wa_uid"]}
                       else:
                            str_profile = ""
                            try:
                                self.ew_db.client_profile.update_one({"phone_number": contact["wa_number"]}, {'$set': {"verified_name": contact["wa_display_name"], "quality_rating": db_client_profile["quality_rating"], "status": str_status, "profile_picture": str_profile, "messaging_status": db_client_profile["messaging_status"], "message_tier": db_client_profile["message_tier"]}})
                            except Exception as e:
                                lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_profile : " + str(e))
                                return jsonify({"error": {"id": "1038", "message": "Data Query Error"}, "success": False})
                            dict_contact = {"verified_name": contact["wa_display_name"], "phone_number": contact["wa_number"], "country_code": str_country_code, "country_code_number": contact["country_code"][1:], "quality_rating": db_client_profile["quality_rating"], "status": str_status, "profile_picture": str_profile, "messaging_status": db_client_profile["messaging_status"], "message_tier": db_client_profile["message_tier"], "wa_uid": db_client_waba_settings["wa_uid"]}
                    else:
                        try:
                            self.ew_db.client_profile.update_one({"phone_number": contact["wa_number"]}, {'$set': {"status": "offline"}})
                        except Exception as e:
                            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_profile : " + str(e))
                            return jsonify({"id": "1039", "message": "Data Query Error", "description": "", "data": "", "success": False})
                        dict_contact = {"verified_name": db_client_profile["verified_name"], "phone_number": contact["wa_number"], "country_code": db_client_profile["country_code"], "country_code_number": contact["country_code"][1:], "quality_rating": db_client_profile["quality_rating"], "status": "offline", "profile_picture": db_client_profile["profile_picture"], "messaging_status": db_client_profile["messaging_status"], "message_tier": db_client_profile["message_tier"], "wa_uid": db_client_waba_settings["wa_uid"]}
                    result_data.append(dict_contact)
            else:
                result_data = None
        return jsonify({"id": "1040", "message": "Phone numbers data fetched successfully", "description": "", "data": result_data, "success": True})            
