"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains function of general messages package only
    * Description: All the general messages function persent here
    
"""

import os
import yaml
import logging
from flask import jsonify

# Import custom packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from ..pkg_extras.mod_common import ClsCommon
from .mod_text_message import ClsSendTextMessage
from .mod_media_message import ClsSendMediaMessage
from .mod_audio_message import ClsAudioMessage
from .mod_contact_message import ClsContactMessage
from .mod_location_message import ClsLocationMessage
from .mod_sticker_message import ClsStickerMessage
from .mod_catalog_messages import ClsCatalogFunction

#calling class objects
obj_common = ClsCommon()






# with open(os.path.dirname(__file__) + '/../conf/logging.yaml', 'r') as f:
#     config = yaml.safe_load(f.read())
#     logging.config.dictConfig(config)
# LOG = logging.getLogger('generalmessagesLog')


class ClsSendAllMessage():
    """ Class for Retriving all messages"""

    def __init__(self):
        """ Create or initialize object and variables """
        pass


    def func_send_all_message(self,data,bot_id,lg):
        """ Send any type of message for the client """
        try:
            ew_db = ClsMongoDBInit.get_ew_db_client()           
            db_client_waba_settings = obj_common.get_waba_settings_by_cc_client_number(data["client_number"])
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB connection error : " + str(e))
            return jsonify({"error": {"id": "2212", "msg": "Data Query Error"}, "success": False})
    
        if db_client_waba_settings == None:
            return jsonify({"msg": "Please enter valid contact number"})
        token = ClsCommon().func_get_access_token(db_client_waba_settings["response"])
        if data["payload"]["message_type"] == "text":
            obj_text = ClsSendTextMessage(data["client_number"])
            resp = obj_text.func_send_text_message(data,db_client_waba_settings["response"],token)
        elif data["payload"]["message_type"] == "image":
            obj_media = ClsSendMediaMessage(data["client_number"])
            resp = obj_media.func_send_media_message_by_link(data,db_client_waba_settings["response"],token)
        elif data["payload"]["message_type"] == "video":
            obj_media = ClsSendMediaMessage(data["client_number"])
            resp = obj_media.func_send_media_message_by_link(data,db_client_waba_settings["response"],token)        
        elif data["payload"]["message_type"] == "document":
            obj_media = ClsSendMediaMessage(data["client_number"])
            resp = obj_media.func_send_media_message_by_link(data,db_client_waba_settings["response"],token)
        elif data["payload"]["message_type"] == "audio":
            obj_audio = ClsAudioMessage(data["client_number"])
            resp = obj_audio.func_audio_message_by_link(data,db_client_waba_settings["response"],token)
        elif data["payload"]["message_type"] == "contact":
            obj_contact = ClsContactMessage(data["client_number"])
            resp = obj_contact.func_contact_message(data,db_client_waba_settings["response"],token)
        elif data["payload"]["message_type"] == "location":            
            obj_location = ClsLocationMessage(data["client_number"]) 
            resp = obj_location.func_location_message(data,db_client_waba_settings["response"],token)
        elif data["payload"]["message_type"] == "sticker":
            obj_sticker = ClsStickerMessage(data["client_number"])
            resp = obj_sticker.func_sticker_message_by_link(data,db_client_waba_settings["response"],token)
        elif data["payload"]["catalog_message_type"] == "spm":            
            obj_catalogue = ClsCatalogFunction(data["client_number"])
            resp = obj_catalogue.func_send_item(data,db_client_waba_settings["response"],token)
        elif data["payload"]["catalog_message_type"] == "mpm":            
            obj_catalogue = ClsCatalogFunction(data["client_number"])
            resp = obj_catalogue.func_send_catalog(data,db_client_waba_settings["response"],token)
        else:
            resp = jsonify({"msg": "Please, enter appropriate message type"})

        return resp