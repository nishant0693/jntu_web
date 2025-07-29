from flask import jsonify
import requests
import pandas as pd
import os
import json
import datetime
from urllib.parse import urlparse
import time

from pymongo import UpdateOne

# Import custom packages
from wacore.pkg_db_connect.mod_db_connection import ClsMongoDBInit
from wacore.pkg_analytics.mod_analytics_function import ClsBroadcastAnalysis
from wacore.pkg_extras.mod_common import ClsCommon
from walogger.walogger import WaLogger

# Initialize logger with name that we want or any other
obj_log = WaLogger('pktmp')
lg = obj_log.get_logger()
obj_common = ClsCommon()


# function send message request to WA Docker
def func_send_message_request(var_message_body,str_recipient_number,str_url_templates,wa_token,client_number,broadcast_id,broadcast_name):#  analytics_arguments added for analytics
    """ Function called to send message request """
    
    obj_clsbroadcastanalysis = ClsBroadcastAnalysis(client_number)
    resp_varmess=[]
    passmess=0
    failmess =0
    update_operations_info = []
    for var_mess in var_message_body:
        lg.info(f"var_message body inside for loop is{var_message_body}")
        number_count = 0
        while number_count < 3:
            dict_headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + wa_token}
            lg.info(f"payload for whatsapi is {var_mess}")
            try:
                var_mess_loads = json.loads(var_mess)
                lg.info(f"var_mess after json loads is {var_mess_loads}")
            except Exception as e:
                lg.info(f"json error at line 42 {e}")
            # response_templates = requests.post(str_url_templates, headers=dict_headers, json=var_mess_loads)
            response_templates = requests.request("POST", str_url_templates, headers=dict_headers, data=var_mess)
            # resp = {"response":""}
            # response_templates = json.dumps(response_templates)
            resp = response_templates.json()
            lg.info(f"whatsapp_api_response is  {resp}")
            if 'errors' in resp and isinstance(resp['errors'], list) and resp['errors']:
                err = resp['errors'][0]
                title = err.get('title', ' ')
                if title=="System overloaded" or title=="Too many requests":
                    time.sleep(300)
                else:
                    pass
            else:
                pass
            if "messages" in resp.keys():
                number_count = 3
            else:
                # time.sleep(300)
                number_count = number_count + 1
        try:

            var_mess_json =json.loads(var_mess)
        except Exception as e:
            lg.info(f"json error at line 57 {e}")

        #old logic commented to add new logic bulk_write
        if "messages" in resp.keys():
            message_response_id = resp["messages"][0]["id"]
            passmess=passmess+1
            update_operation = {"broadcast_id": broadcast_id,
                "recipient_number":var_mess_json["to"],
                "message_response_id":message_response_id,
                "sent":True,
                "whatsappresp":resp}
                            
            
        else:
            if 'error' in resp:
                err = resp['error']
                title = err.get('message', 'Invalid Number')
                message_response_id = "None"
                failmess=failmess+1
                update_operation = {"broadcast_id": broadcast_id,
                    "recipient_number":var_mess_json["to"],
                    "message_response_id":message_response_id,"sent":"NA","delivered":"NA","read":"NA","reason": title,"whatsappresp":resp
                }
            else:
                message_response_id = "None"
                failmess=failmess+1
                update_operation = {"broadcast_id": broadcast_id,
                    "recipient_number":var_mess_json["to"],
                    "message_response_id":message_response_id,"sent":"NA","delivered":"NA","read":"NA","reason": "Invalid Number","whatsappresp":resp
                }

            lg.info(f"value of fail_mess is {failmess}")
        resp_varmess.append(update_operation)     
        """
        #commented as only one record is getting inserted 
        if "messages" in resp.keys():
            message_response_id = resp["messages"][0]["id"]
            passmess += 1
            filter_criteria = {"broadcast_id": broadcast_id}
            update_operation = {"$set": {
                "recipient_number": var_mess_json["to"],
                "message_response_id": message_response_id,
                "sent": True,
                "whatsappresp": resp
            }}
            update_operations_info.append(UpdateOne(filter_criteria, update_operation,upsert=True))
        else:
            message_response_id = "None"
            failmess += 1
            filter_criteria = {"broadcast_id": broadcast_id}
            update_operation= {"$set": {
                    "recipient_number": var_mess_json["to"],
                    "message_response_id": message_response_id,
                    "sent": "NA",
                    "delivered": "NA",
                    "read": "NA",
                    "reason": "Invalid Number",
                    "whatsappresp": resp
                }}
            update_operations_info.append(UpdateOne(filter_criteria, update_operation,upsert=True))
            lg.info(f"value of fail_mess is {failmess}")
            """
    #-------------Added for Analytics-------------------------
    
        lg.info(f"sending it func_broadcast_stats_valid {resp}")
    lg.info(f"upddat_opertaions_info is {update_operations_info}")
    obj_clsbroadcastanalysis.func_broadcast_stats_valid(broadcast_id,broadcast_name,resp,client_number,str_recipient_number,var_mess,resp_varmess,passmess,failmess)

    #------------------------Analytics End----------------------

    # if 'errors' in  response_templates.json():
    #     resp = response_templates.json()['errors'][0]['title']
    return True