"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains function of general messages package only
    * Description: All the audio messages function present here 
"""

import requests
import json
from flask import jsonify

#import custon packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from ..pkg_extras.mod_common import ClsCommon
from ..pkg_analytics.mod_analytics_function import ClsDmpServiceAnalytics

#Creating class object



class ClsAudioMessage():
    """ Class for Retriving audio message functions """

    def __init__(self,client_number):
        """ Create or initialize object and variables """
        self.client_number = client_number
        self.obj_analytics = ClsDmpServiceAnalytics(self.client_number)
        pass
    

    def func_audio_message_by_id(self,str_client_number,var_file_data,str_recipient_number):
        """ Function to send audion message by id """
        
        try:
            ew_db=ClsMongoDBInit.get_db_client()
            db_client_waba_settings = ew_db.client_waba_settings.find_one({"client_number": data["client_number"]}, {"_id":0})
        except Exception as e:
            return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
        finally:
            ew_db.client.close()
        if db_client_waba_settings == None:
            return jsonify({"msg": "Please enter valid contact number linked with bot id"})
        str_token = ClsCommon().get_access_token(db_client_waba_settings)

        if var_file_data.content_type.endswith('mpeg'):
                str_content_type = "audio/mpeg"
        else:
            return jsonify({'msg': 'Invalid File Format'}) 

        url = db_client_waba_settings["url"] + "media"
        payload = var_file_data
        headers = {'Content-Type': str_content_type, 'Authorization': 'Bearer ' + str_token}
        response = requests.request("POST", url, headers=headers, data=payload)
        if 'errors' not in response.json().keys():
            var_send_url = db_client_waba_settings["url"] + "messages"
            dict_media_id = {"id": response.json()["media"][0]["id"]}
            var_send_payload = json.dumps({"to": str_recipient_number, "type": "audio", "recipient_type": "individual", "audio": dict_media_id})
            var_send_headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + str_token}

            send_response = requests.request("POST", var_send_url, headers=var_send_headers, data=var_send_payload)
            data = {"recipient_number":str_recipient_number,"client_number":str_client_number}
            self.obj_analytics.func_general_messages_analytics(send_response,data,"audio")  #for analytics
        else:
            return jsonify({"msg": "Invalid media format"})
        
        return jsonify({"response": "Message sent successfully"})        


    def func_audio_message_by_link(self,data,db_client_waba_settings,str_token):
        """ Function called to send audio message by url"""
        var_send_url = db_client_waba_settings["url"] + "messages"
        dict_media = {"link": data["payload"]["media_link"]}
        var_send_payload = json.dumps({"to": data["recipient_number"], "type": "audio", "recipient_type": "individual", "audio": dict_media})
        var_send_headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + str_token}
        send_response = requests.request("POST", var_send_url, headers=var_send_headers, data=var_send_payload)
        if "errors" not in send_response.json().keys():

            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_analytics(send_response,data,"audio") 
            #------------------Analytics End----------
            
            return jsonify({"response": "Message sent successfully"})
        else:   

            #--------------Added for Analytics---------
            self.obj_analytics.func_general_messages_invalid(send_response,data)
            #------------------Analytics End----------

            return jsonify({"msg": "Please check media link or contact"})