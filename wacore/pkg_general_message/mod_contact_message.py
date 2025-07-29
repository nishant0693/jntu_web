"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains function of general messages package only
    * Description: All the contact messages function present here 
"""

import requests
import json
from flask import jsonify

#Import custom packages
from ..pkg_analytics.mod_analytics_function import ClsDmpServiceAnalytics

#Creating class object



class ClsContactMessage():
    """ Class for Retriving contact message functions """

    def __init__(self,client_number):
        """ Create or initialize object and variables """
        self.client_number = client_number
        self.obj_analytics = ClsDmpServiceAnalytics(self.client_number)
        pass
    

    def func_contact_message(self,data,db_client_waba_settings,str_token):
        """ Function called to send contact message """
        """
            "contacts" : [
                {
                    "addresses": 
                    [
                        {
                            "city": "<Contact's City>",
                            "country": "<Contact's Country>",
                            "country_code": "<Contact's Country Code>",
                            "state": "<Contact's State>",
                            "street": "<Contact's Street>",
                            "type": "<Contact's Address Type>",
                            "zip": "<Contact's Zip Code>"
                        }
                    ],
                    "birthday": "<Contact's Birthday>",
                    "emails": 
                        [
                            {
                                "email": "<Contact's Email>",
                                "type": "<Contact's Email Type>"
                            }
                        ],
                    "ims": [],
                    "name": 
                    {
                        "first_name": "<Contact's First Name>",
                        "formatted_name": "<Contact's Formatted Name>",
                        "last_name": "<Contact's Last Name>"
                    },
                    "org": 
                    {
                        "company": "<Contact's Company>"
                    },
                    "phones": 
                    [
                        {
                            "phone": "<Contact's Phone Number>",
                            "type": "<Contact's Phone Number Type>"
                        }
                    ],
                    "urls": []
                }
            ]
        
        """
        var_send_url = db_client_waba_settings["url"] + "messages"
        dict_contacts = data["payload"]["contacts"]
        var_send_payload = json.dumps({"to": data["recipient_number"], "type": "contacts", "recipient_type": "individual", "contacts": dict_contacts})
        var_send_headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + str_token}
        send_response = requests.request("POST", var_send_url, headers=var_send_headers, data=var_send_payload)
        if "errors" in send_response.json().keys():
            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_invalid(send_response,data)
            #------------------Analytics End----------
            return jsonify({"msg": "Incorrect contact json. Please enter appropriate format"})
        else:
            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_analytics(send_response,data, "contact")   #for analytics 
            #--------------Added for Analytics---------        
            return jsonify({"response": "Message sent successfully"})