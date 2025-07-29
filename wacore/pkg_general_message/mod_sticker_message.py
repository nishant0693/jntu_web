"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains function of general messages package only
    * Description: All the sticker messages function present here  
"""

import requests
import json
from flask import jsonify

# Import custom packages
from ..pkg_analytics.mod_analytics_function import ClsDmpServiceAnalytics

# Calling class objects



class ClsStickerMessage():
    """ Class for Retriving sticker message functions """

    def __init__(self,client_number):
        """ Create or initialize object and variables """
        self.client_number = client_number
        self.obj_analytics = ClsDmpServiceAnalytics(self.client_number)
        pass


    def func_sticker_message_by_link(self,data,db_client_waba_settings,str_token):
        """ Function called to send sticker message by link"""
        var_send_url = db_client_waba_settings["url"] + "messages"
        dict_sticker = {"link": data["payload"]["sticker_link"]}
        var_send_payload = json.dumps({"to": data["recipient_number"], "type": "sticker", "recipient_type": "individual", "sticker": dict_sticker})
        var_send_headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + str_token}
        send_response = requests.request("POST", var_send_url, headers=var_send_headers, data=var_send_payload)
        
        if "errors" not in send_response.json().keys():  
            #--------------Added for Analytics---------          
            self.obj_analytics.func_general_messages_analytics(send_response,data,"sticker") #for analytics
            #------------------Analytics End----------
            return jsonify({"response": "Message sent successfully"})
        else:
            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_invalid(send_response,data)
            #------------------Analytics End----------
            return jsonify({"msg": "Please check sticker link or contact"})
