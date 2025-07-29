"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains function of general messages package only
    * Description: All the catalogue messages function present here 
"""

import json
import requests
from flask import jsonify
from ..pkg_analytics.mod_analytics_function import ClsDmpServiceAnalytics

#Creating class object


class ClsCatalogFunction():
    """ Class for Retriving catalogue message functions """

    def __init__(self,client_number):
        """ Class of routes used for analytics 
            
        Initialize required object and variables:

        Parameters:
            client_number (str): Mobile Number of WhatsApp 
            db_client_waba_settings (dict): All WABA Settings         
        Local Variable:
            client_db_name(str): Fetch and generate DB name for individual client
            ew_db(DB object): Initialize DB
            client_db(DB Object):  Initialize DB
            data : data from hte request body
            str_catalog_id :  id of the catalog from which 
            str_header_text : text to be sent in header of the product message
            str_body_text : text to be sent in body of the product message
            str_footer_text : text to be sent as footer of the product message
            send_response : response recieved when message is sent
            Returns:
            All above parameteres and variables
      
        """
        self.client_number = client_number
        self.obj_analytics = ClsDmpServiceAnalytics(self.client_number)
        pass


    def func_send_item(self,data,db_client_waba_settings,str_token):
        """Function for sending a single product message"""
        recipient_number = data["recipient_number"]
        str_catalog_id = data["payload"]["catalog_id"]
        str_body_text = data["payload"]["body_text"]
        str_footer_text = data["payload"]["footer_text"]
        str_product_retailer_id =  data["payload"]["product_retailer_id"]

        if type(data["payload"]["product_retailer_id"]) == list:
            return jsonify({"msg":"Please check catalog message type or product retailer ids."}) 

        url = db_client_waba_settings['url'] + "messages/"
        payload = json.dumps({ 
        "recipient_type": "individual",
        "to" : recipient_number,
        "type": "interactive",
        "interactive": {
        "type": "product",
        "body": {"text": str_body_text},
        "footer": {"text": str_footer_text},
        "action": {"catalog_id":str_catalog_id,"product_retailer_id": str_product_retailer_id}}})
       
        headers = {'Content-Type': 'application/json','Authorization': 'Bearer '+ str_token}
        send_response = requests.request("POST", url, headers=headers, data=payload)

        if "errors" in send_response.json().keys():

            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_invalid(send_response,data)
            #------------------Analytics End----------
            
            return jsonify({"msg": "Incorrect contact json. Please enter appropriate format"})

        else:

            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_analytics(send_response,data, "spm")            
            #------------------Analytics End----------

            return jsonify({"response": "Message sent successfully"})


    def func_send_catalog(self,data,db_client_waba_settings,str_token):
        """Function for sending a multi-product message"""
        recipient_number = data["recipient_number"]
       
        str_catalog_id = data["payload"]["catalog_id"]
        str_header_text = data["payload"]["header_text"]
        str_body_text = data["payload"]["body_text"]
        str_footer_text = data["payload"]["footer_text"]
        list_section = list(data["payload"]["section"])
        message_section = []
        for section in list_section:
            product_items =[]
            for product_id in section["product_retailer_id"]:                 
                product_items.append({"product_retailer_id": product_id})                     
            section_dictionary = {"title": section["title"],"product_items":product_items}  
            message_section.append(section_dictionary)
                                
        url = db_client_waba_settings['url'] + "messages/"       
        payload = json.dumps({"recipient_type": "individual","to" : recipient_number,"type": "interactive", "interactive": {"type": "product_list", "header":{"type": "text","text": str_header_text}, "body": {"text": str_body_text }, "footer": {"text": str_footer_text}, "action": {"catalog_id":str_catalog_id, "sections":message_section}}})                                    
        headers = {'Content-Type': 'application/json','Authorization': 'Bearer '+ str_token}
        send_response = requests.request("POST", url, headers=headers, data=payload)
              
        if "errors" in send_response.json().keys():

            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_invalid(send_response,data)
            #------------------Analytics End----------
            
            return jsonify({"msg": "Incorrect contact json. Please enter appropriate format or check the number products in message"})
        else:
            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_analytics(send_response,data, "mpm")            
            #------------------Analytics End----------
            return jsonify({"response": "Message sent successfully"})



  