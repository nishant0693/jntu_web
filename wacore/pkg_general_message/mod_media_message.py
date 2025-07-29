"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains function of general messages package only
    * Description: All the media messages function present here
    
"""

import requests
import json
from flask import jsonify

# Import custom packages
from ..pkg_extras.mod_common import ClsCommon
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from ..pkg_analytics.mod_analytics_function import ClsDmpServiceAnalytics

# Calling class objects

class ClsSendMediaMessage():
    """ Class for Retriving media message functions """
    def __init__(self,client_number):
        """ Create or initialize object and variables """
        self.client_number = client_number
        self.obj_analytics = ClsDmpServiceAnalytics(self.client_number)

        pass


    def func_send_media_message_by_id(self,str_client_number,var_file_data,str_filename,str_recipient_number,str_media_type):
        """ Function called to send media message by id """
        try:
            ew_db=ClsMongoDBInit.get_db_client()
            db_client_waba_settings = ew_db.client_waba_settings.find_one({"client_number": str_client_number}, {"_id":0})
        except Exception as e:
            return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
        finally:
            ew_db.client.close()
        
        if db_client_waba_settings == None:
            return jsonify({"msg": "Please enter valid contact number linked with bot id"})
        str_token = ClsCommon().get_access_token(db_client_waba_settings)

        if str_media_type == "image":
            if var_file_data.content_type.endswith('png') or var_file_data.content_type.endswith('jpg'):
                str_content_type = "image/jpeg"
            else:
                return jsonify({'msg': "File format for image must be .png or .jpg"})
        elif str_media_type == "video":
            if var_file_data.content_type.endswith('mp4'):
                str_content_type = "video/mp4"
            else:
                return jsonify({'msg': "File format for video must be .mp4"})
        elif str_media_type == "document":
            if var_file_data.content_type.endswith('pdf'):
                str_content_type = "application/pdf"
            else:
                return jsonify({'msg': "File format for document must be .pdf"})
        else:
            return jsonify({'msg': 'Invalid File Format'}) 
        
        url = db_client_waba_settings["url"] + "media"
        payload= var_file_data
        headers = {'Content-Type': str_content_type, 'Authorization': 'Bearer ' + str_token}
        response = requests.request("POST", url, headers=headers, data=payload)
        if 'errors' not in response.json().keys():
            var_send_url = db_client_waba_settings["url"] + "messages"
            if str_media_type == 'document':
                dict_media_id = {"id": response.json()["media"][0]["id"], "filename": str_filename}
            else:
                dict_media_id = {"id": response.json()["media"][0]["id"]}
            var_send_payload = json.dumps({"to": str_recipient_number, "type": str_media_type, "recipient_type": "individual", str_media_type: dict_media_id})
            var_send_headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + str_token}

            send_response = requests.request("POST", var_send_url, headers=var_send_headers, data=var_send_payload)
            
            # -------------------Added for analytics--------------
            
            data = {"recipient_number": str_recipient_number, "client_number": str_client_number}
            self.obj_analytics.func_general_messages_analytics(send_response,data,str_media_type)
            
            # -------------------Analytics Ends------------------
        else:
            return jsonify({"msg": "Invalid media format"})
        
        return jsonify({"response": "Message sent successfully"})


    def func_send_media_message_by_link(self,data,db_client_waba_settings,str_token):
        """ Function called to send media message by url"""
        var_send_url = db_client_waba_settings["url"] + "messages"
        if data["payload"]["message_type"] == "image":
            dict_media = {"link": data["payload"]["media_link"]}
        elif data["payload"]["message_type"] == "video":
            dict_media = {"link": data["payload"]["media_link"]}    
        elif data["payload"]["media_link"]:
            if "filename" not in data["payload"]:
                return jsonify({"msg": "Please enter filename for message type document."})
            else:
                dict_media = {"link": data["payload"]["media_link"], "filename": data["payload"]["filename"]}
        var_send_payload = json.dumps({"to": data["recipient_number"], "type": data["payload"]["message_type"], "recipient_type": "individual", data["payload"]["message_type"]: dict_media})
        var_send_headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + str_token}
        send_response = requests.request("POST", var_send_url, headers=var_send_headers, data=var_send_payload)
        if "errors" not in send_response.json().keys(): 
            #--------------Added for Analytics--------- 
            self.obj_analytics.func_general_messages_analytics(send_response,data,data["payload"]["message_type"])  #for analytics
            #-------------- Analytics End--------------
            return jsonify({"response": "Message sent successfully"})
        else:
            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_invalid(send_response,data)
            #------------------Analytics End-----------
            return jsonify({"msg": "Please check media link or contact"})