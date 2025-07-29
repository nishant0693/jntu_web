"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains function text messages of general messages package only
    * Description: All the text messages function persent here
    
"""

import json
from flask import jsonify
import logging.config
import logging
import os.path
import yaml
import requests

# Import custom packages
from ..pkg_analytics.mod_analytics_function import ClsDmpServiceAnalytics

# Calling class objects

# with open(os.path.dirname(__file__) + '/../conf/logging.yaml', 'r') as f:
#     config = yaml.safe_load(f.read())
#     logging.config.dictConfig(config)
# LOG = logging.getLogger('generalmessagesLog')


class ClsSendTextMessage():
    """ Class for Retriving text message functions """
    
    def __init__(self,client_number):
        """ Create or initialize object and variables """
        self.client_number = client_number
        self.obj_analytics = ClsDmpServiceAnalytics(self.client_number)
        pass


    def func_send_text_message(self,data,db_client_waba_settings,token):
        """ Function called for sending text message logic  """
        var_url = db_client_waba_settings["url"] + "messages"
        var_payload= json.dumps({"recipient_type": "individual", "to": data["recipient_number"], "type": "text", "text": {"body": data["payload"]["message_text"]}})
        var_headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
        response = requests.request("POST", var_url, headers=var_headers, data=var_payload)
        if "errors" not in response.json().keys():
            #--------------Added for Analytics---------
            

            self.obj_analytics.func_general_messages_analytics(response,data,"text")   #for analytics
            #------------------Analytics End----------
            return jsonify({"response": "Message sent successfully"})
        else:
            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_invalid(response,data)
            #------------------Analytics End----------
            return jsonify({"msg": "Please check message content or contact"})

