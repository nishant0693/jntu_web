"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains function of general messages package only
    * Description: All location media messages function present here 
"""

import requests
import json
from flask import jsonify

#Import custom packages
from ..pkg_analytics.mod_analytics_function import ClsDmpServiceAnalytics

#Calling class objects


class ClsLocationMessage():
    """ Class for Retriving location message functions """

    def __init__(self,client_number):
        """ Create or initialize object and variables """
        self.client_number = client_number
        self.obj_analytics = ClsDmpServiceAnalytics(self.client_number)
        pass
    

    def func_location_message(self,data,db_client_waba_settings,str_token):
        """ Function called to send location message """
        var_send_url = db_client_waba_settings["url"] + "messages"
        if "longitude" not in data["payload"] or "latitude" not in data["payload"]:
            return jsonify({"msg":"Please enter appropriate longitude and latitude."})
            
        dict_location = {"longitude": data["payload"]["longitude"], "latitude": data["payload"]["latitude"]}
        if "name" in data["payload"]:
            dict_location.update({"name": data["payload"]["name"]})
        if "address" in data["payload"]:
            dict_location.update({"address": data["payload"]["address"]})
        var_send_payload = json.dumps({"to": data["recipient_number"], "type": "location", "recipient_type": "individual", "location": dict_location})
        var_send_headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + str_token}
        send_response = requests.request("POST", var_send_url, headers=var_send_headers, data=var_send_payload)
        if "errors" not in send_response.json().keys():
            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_analytics(send_response,data,"location")  #for analytics
            #-------------- Analytics End---------
            return jsonify({"response": "Message sent successfully"})
        else:
            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_invalid(send_response,data)
            #------------------Analytics End----------
            return jsonify({"msg": "Please check location or contact"})


