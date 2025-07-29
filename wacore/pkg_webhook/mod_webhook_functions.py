"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains schemas of DMP webhook package only
    * Description: All the DMP webhook functions present here
"""

# import yaml
import logging
import logging.config
import os.path
import requests
from flask import jsonify,json
# from datetime import datetime
import datetime
from requests.auth import HTTPBasicAuth
import time
import ast

import re

# Import custom packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
# from ..pkg_analytics.mod_analytics_function import  ClsButtonResponse
# from ..pkg_analytics.mod_analytics_function import  ClsStatusUpdate ,ClsButtonResponse
from wacore.pkg_extras.mod_common import ClsCommon

from wacore.pkg_template_broadcast.mod_celery_broadcast2 import func_send_message_request 

from waapi.celery_worker import AsyncSendtoNewWebhook
from walogger.walogger import WaLogger
obj_log = WaLogger('pktmp')
lg = obj_log.get_logger()

obj_common = ClsCommon()
# with open(os.path.dirname(__file__) + '/../conf/logging.yaml', 'r') as f:
#     config = yaml.safe_load(f.read())
#     logging.config.dictConfig(config)
# LOG = logging.getLogger('webhookLog')

class ClsWebhook:
    """ Class for webhook functions """

    def __init__(self,client_number):
        """ Class for recording button responses from users 
            
        Initialize required object and variables:

        Parameters:
            client_number (str): Mobile Number of WhatsApp 
            db_client_waba_settings (dict): All WABA Settings         
        Local Variable:
            client_db_name(str): Fetch and generate DB name for individual client
            ew_db(DB object): Initialize DB
            client_db(DB Object):  Initialize DB
            str_status (str) : status received on webhook (seesnt,delivered,read)
            data_keys (list): list of keys in data received from webhook

            Returns:
            All above parameteres and variables      
        """
        #lg.info("dev in class")
        self.client_number = client_number
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        db_client_waba_settings = obj_common.get_waba_settings_by_cc_client_number(self.client_number)
        if "error" in db_client_waba_settings:
            return jsonify({"error": "Invalid client number. Please enter appropriate client number"})    
        self.client_db_name = str(db_client_waba_settings["response"]["ew_id"]).lower() + "_" + db_client_waba_settings["response"]["waba_id"]       
        self.client_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)

    def func_webhook(self,data):
        lg.info(f" recevied data -----------{data}")
        # self.client_db.webhook_status.insert_one({"data":data})
        from ..pkg_analytics.mod_analytics_function import ClsStatusUpdate, ClsButtonResponse
        """ Function for updates received on webhook regarding status updates and button responses """
        #lg.info(f"After hitting data_key{type(data)} yo ho {data}")
        data_keys = list(data.keys())

        # self.client_db.data_keys.insert_one({"client_number":self.client_number,"status":data_keys})
        statuses_exist = False
        for entry in data.get('entry', []): 
            for change in entry.get('changes', []):  
                if 'value' in change and isinstance(change['value'], dict):
                    if 'statuses' in change['value']:
                        statuses_exist = True
                        str_status = change['value']['statuses'][0]['status']
                        msg_id = change['value']['statuses'][0]['id']
                        lst_statuses = change['value']['statuses'][0]

                        break
                    elif 'messages' in change['value']:
                        profile_name = change['value']['contacts'][0]['profile']['name']
                        wa_id = change['value']['contacts'][0]['wa_id']
                        lg.info(f"value of profile_name is {profile_name} and value of wa_id is {wa_id}")
                        messages = change['value']['messages']
                        contacts = change['value']['contacts']
                for message in messages:
                    if message.get('type') == 'button':
                        try:
                            button_payload = message.get('button', {}).get('payload', None)
                            button_text = message.get('button', {}).get('text', None)
                            button_id = message['context']['id']
                            lg.info(f"Button_payload is {button_payload} type is {type(button_payload)}")
                            button_payload = ast.literal_eval(button_payload)
                            len_journey = len(button_payload.get('J', ''))
                            lg.info(f"length of journey is {len_journey}")
                            
                            if 'J' in button_payload and len_journey != 0:
                                data = {
                                    "contacts": contacts,
                                    "messages": [
                                        {
                                            "button": {
                                                "payload": button_payload,
                                                "text": messages[0]['button']['text']
                                            },
                                            "context": messages[0]['context'],
                                            "from": messages[0]['from'],
                                            "id": messages[0]['id'],
                                            "timestamp": messages[0]['timestamp'],
                                            "type": messages[0]['type']
                                        }
                                    ]
                                } 
                                payload = json.dumps(data)
                                db_client_waba_settings=obj_common.get_waba_settings_by_cc_client_number(self.client_number)
                                response_data = db_client_waba_settings.get("response", {})
                                clientname = response_data.get("int_id")
                                clientid=response_data.get("ew_id")
                                url="http://192.168.254.92:9050/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                                headers = {'X-Wa-Account-Id': '""','Content-Type': 'application/json'}
                                response = requests.request("POST", url, headers=headers, data=payload)
                                lg.info("response is {reponse.text}")
                                clsbuttonresponse_obj = ClsButtonResponse(self.client_number)
                                clsbuttonresponse_obj.func_button_response(data,button_id,button_text)
                            else:
                                clsbuttonresponse_obj = ClsButtonResponse(self.client_number)
                                clsbuttonresponse_obj.func_button_response(data,button_id,button_text)
                        except Exception as e:
                            self.client_db.button_error.insert_one({"client_number":self.client_number,"status":str(e)})

                           
                        break
                    elif message.get('type') == 'text':
                        current_timestamp = int(datetime.datetime.now().timestamp())
                        user_message_id = message.get('id')
                        user_message_timestamp = message.get('timestamp','')
                        user_message_text = message.get('text', {}).get('body', None)
                        user_mobile_number =  message.get('from')
                        lg.info(f"user mobile_number is user {user_mobile_number}")
                        # lg.info(f"client_nuberis{self.client_number}")
                        db_client_waba_settings=obj_common.get_waba_settings_by_cc_client_number(self.client_number)
                        # lg.info(f"client_nuberis{self.client_number}")
                        response_data = db_client_waba_settings.get("response", {})
                        # lg.info(f"response_datais{response_data}")
                        clientname = response_data.get("int_id")
                        clientid=response_data.get("ew_id")
                        lg.info(f"clientnameis{clientname}")
                        self.client_db.converstaional_data_collection.insert_one({"client_number":self.client_number,"message_response_id":user_message_id,"user_message":user_message_text,"user_mobile_number":user_mobile_number,"timestamp":user_message_timestamp,"session_id":current_timestamp,"int_id":clientname,"clientid":clientid,"cost":0.8})
                        url="http://192.168.254.92:9050/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        lg.info(f"url trying to hit is {url}")
                        data = {"contacts": [{"profile": {"name":profile_name }, "wa_id": str(wa_id)}], "messages": [{"from": str(wa_id), "id":str(user_message_id) , "text": {"body":str(user_message_text)}, "timestamp":str(user_message_timestamp), "type": "text"}]}
                        payload = json.dumps(data)
                        headers = {'X-Wa-Account-Id': '""','Content-Type': 'application/json'}
                        try:
                            response = requests.request("POST", url, headers=headers, data=payload)
                            lg.info(f"response is {response.text}")
                        except Exception as e:
                            lg.info(f"error is {str(e)}")
                        # self.client_db.omni_test.insert_one({"url":url,"payload":data,"response":response.text})
                    elif message.get('type') == 'interactive':
                        # interactive = message['interactive']['list_reply']
                        interactive = message['interactive']
                        if 'list_reply' in interactive:
                            interactive = message['interactive']['list_reply']
                            data = {
                                "contacts": contacts,
                                "messages": [
                                    {"context": message['context'],
                                        "from": message['from'],
                                        "id": message['id'],

                                        "interactive": {
                                            "list_reply": {
                                                "description": interactive.get('description', ''),
                                                "id": interactive['id'],
                                                "title": interactive['title']
                                            },
                                            "type": message['interactive']['type']
                                        },
                                        "timestamp": message['timestamp'],
                                        "type": message['type'],
                                        
                                    }
                                ]
                            }
                        elif 'button_reply' in interactive:
                            interactive = message['interactive']['button_reply']
                            data = {
                                "contacts": contacts,
                                "messages": [
                                    {"context": message['context'],
                                        "from": message['from'],
                                        "id": message['id'],

                                        "interactive": {
                                            "button_reply": {
                                                "description": interactive.get('description', ''),
                                                "id": interactive['id'],
                                                "title": interactive['title']
                                            },
                                            "type": message['interactive']['type']
                                        },
                                        "timestamp": message['timestamp'],
                                        "type": message['type'],
                                        
                                    }
                                ]
                            }
                        
                        elif 'nfm_reply' in interactive:
                            lg.info(f"inside flow data")
                            interactive = message['interactive']['nfm_reply']
                            lg.info(f"raw flow data is {interactive}")
                            response_json_str = interactive.get('response_json', {})
                            # response_json = json.loads(response_json_str)
                            response_json = json.loads(response_json_str)
                            # response_list = list(response_json.items())
                            simplified_flow_data = []
                            for key, value in response_json.items():
                                lg.info(f"key that is checking is {key}")
                                match = re.search(r"screen_\d+_(.*?)_\d+", key)
                                simplified_key = match.group(1) if match else key
                                if isinstance(value, list):
                                    simplified_values = []
                                    for v in value:
                                        match_value = re.search(r"_(.*)", v)
                                        simplified_values.append(match_value.group(1) if match_value else v)
                                    simplified_flow_data.append([simplified_key, simplified_values])
                                else:
                                    match_value = re.search(r"_(.*)", value)
                                    simplified_value = match_value.group(1) if match_value else value
                                    simplified_flow_data.append([simplified_key, simplified_value])
                                                    
                            chat_simplified_flow_data = ', '.join(f"{key}: {value}" for key, value in simplified_flow_data if key != 'flow_token')
                            lg.info(f"flow data in string is{ chat_simplified_flow_data}")

                            import datetime

                            current_timestamp = int(datetime.datetime.now().timestamp())
                            dt_object_cur = datetime.datetime.utcfromtimestamp(current_timestamp)
                            date_cur = dt_object_cur.strftime("%d")
                            month_cur = dt_object_cur.strftime("%m")
                            year_cur = dt_object_cur.strftime("%Y")
                            datemonth_cur = str(date_cur+month_cur+year_cur)
                            user_message_timestamp = message.get('timestamp','')
                            mobile_number =  message['from']
                            product_entry = {'flow_data': simplified_flow_data,'mobile_number': mobile_number,"date":datemonth_cur,"timestamp":user_message_timestamp}
                            self.client_db.user_flow_data.insert_one(product_entry)
                            user_message_id = message.get('id')
                            user_message_timestamp = message.get('timestamp','')
                            # user_message_text = "Submit"
                            user_message_text = chat_simplified_flow_data
                            user_mobile_number =  message.get('from')
                            db_client_waba_settings=obj_common.get_waba_settings_by_cc_client_number(self.client_number)
                            response_data = db_client_waba_settings.get("response", {})
                            clientname = response_data.get("int_id")
                            clientid=response_data.get("ew_id")
                            url=  "http://192.168.254.92:9050/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        # url="https://whatsappservices.engagelybots.ai/testengagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        # url = "https://whatsapptest.engagelybots.ai/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                            data = {"contacts": [{"profile": {"name":profile_name }, "wa_id": str(wa_id)}], "messages": [{"from": str(wa_id), "id":str(user_message_id) , "text": {"body":str(user_message_text)}, "timestamp":str(user_message_timestamp), "type": "text"}]}
                            payload = json.dumps(data)
                            headers = {'X-Wa-Account-Id': '""','Content-Type': 'application/json'}
                            # response = requests.request("POST", url, headers=headers, data=payload)


                        payload = json.dumps(data)
                        db_client_waba_settings=obj_common.get_waba_settings_by_cc_client_number(self.client_number)
                        response_data = db_client_waba_settings.get("response", {})
                        clientname = response_data.get("int_id")
                        clientid=response_data.get("ew_id")
                        url="http://192.168.254.92:9050/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        headers = {'X-Wa-Account-Id': '""','Content-Type': 'application/json'}
                        response = requests.request("POST", url, headers=headers, data=payload)
                        lg.info("response is {reponse.text}")
                    elif message.get('type') == 'image':
                        lg.info(f"inside image checking one")
                        clsstatusupdate_obj = ClsStatusUpdate(self.client_number)
                        user_message_id = message.get('id')
                        user_message_timestamp = message.get('timestamp','')
                        media_id = message.get('image', {}).get('id', None)
                        sha_256 =  message.get('image', {}).get('sha256', None)
                        lg.info(f"media_id is {media_id}")
                        user_mobile_number =  message.get('from')

                        lg.info(f"sha_256 value is {sha_256}")
                        
                        lg.info(f"user mobile_number is user {user_mobile_number}")
                        db_client_waba_settings=obj_common.get_waba_settings_by_cc_client_number(self.client_number)
                        response_data = db_client_waba_settings.get("response", {})
                        clientname = response_data.get("int_id")
                        clientid=response_data.get("ew_id")
                        # self.client_db.converstaional_data_collection.insert_one({"client_number":self.client_number,"message_response_id":user_message_id,"user_message":user_message_text,"user_mobile_number":user_mobile_number,"timestamp":user_message_timestamp,"session_id":user_message_timestamp,"int_id":clientname,"clientid":clientid,"cost":0.8})
                        # url="https://whatsappservices.engagelybots.ai/testengagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        # url = "https://whatsapptest.engagelybots.ai/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        
                        url= "http://192.168.254.92:9050/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        
                        data = {'contacts': [{'profile': {'name': profile_name}, 'wa_id': str(wa_id)}], 'messages': [{'from': str(wa_id), 'id': str(user_message_id), 'image': {'id': media_id, 'mime_type': 'image/jpeg', 'sha256': sha_256, 'status': 'downloaded'}, 'timestamp': user_message_timestamp, 'type': 'image'}]}
                        payload = json.dumps(data)
                        # data = {"contacts": [{"profile": {"name":profile_name }, "wa_id": str(wa_id)}], "messages": [{"from": str(wa_id), "id":str(user_message_id) , "text": {"body":str(user_message_text)}, "timestamp":str(user_message_timestamp), "type": "text"}]}

                        headers = {'X-Wa-Account-Id': '""','Content-Type': 'application/json'}
                        response = requests.request("POST", url, headers=headers, data=payload,timeout=5)
                        lg.info(f"response is {response.text}")
                    
                    elif message.get('type') == 'video':
                        lg.info(f"inside image checking one")
                        clsstatusupdate_obj = ClsStatusUpdate(self.client_number)
                        user_message_id = message.get('id')
                        user_message_timestamp = message.get('timestamp','')
                        media_id = message.get('video', {}).get('id', None)
                        sha_256 =  message.get('video', {}).get('sha256', None)
                        lg.info(f"media_id is {media_id}")
                        user_mobile_number =  message.get('from')

                        lg.info(f"sha_256 value is {sha_256}")
                        
                        lg.info(f"user mobile_number is user {user_mobile_number}")
                        db_client_waba_settings=obj_common.get_waba_settings_by_cc_client_number(self.client_number)
                        response_data = db_client_waba_settings.get("response", {})
                        clientname = response_data.get("int_id")
                        clientid=response_data.get("ew_id")
                        # self.client_db.converstaional_data_collection.insert_one({"client_number":self.client_number,"message_response_id":user_message_id,"user_message":user_message_text,"user_mobile_number":user_mobile_number,"timestamp":user_message_timestamp,"session_id":user_message_timestamp,"int_id":clientname,"clientid":clientid,"cost":0.8})
                        # url="https://whatsappservices.engagelybots.ai/testengagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        # url = "https://whatsapptest.engagelybots.ai/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        
                        url="http://192.168.254.92:9050/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        
                        data = {'contacts': [{'profile': {'name': profile_name}, 'wa_id': str(wa_id)}], 'messages': [{'from': str(wa_id), 'id': str(user_message_id), 'video': {'id': media_id, 'mime_type': 'video/mp4', 'sha256': sha_256, 'status': 'downloaded'}, 'timestamp': user_message_timestamp, 'type': 'video'}]}
                        payload = json.dumps(data)
                        # data = {"contacts": [{"profile": {"name":profile_name }, "wa_id": str(wa_id)}], "messages": [{"from": str(wa_id), "id":str(user_message_id) , "text": {"body":str(user_message_text)}, "timestamp":str(user_message_timestamp), "type": "text"}]}

                        headers = {'X-Wa-Account-Id': '""','Content-Type': 'application/json'}
                        response = requests.request("POST", url, headers=headers, data=payload,timeout=5)
                        lg.info(f"response is {response.text}")
                    

                    elif message.get('type') == 'document':
                        lg.info(f"inside document checking one")
                        clsstatusupdate_obj = ClsStatusUpdate(self.client_number)
                        user_message_id = message.get('id')
                        user_message_timestamp = message.get('timestamp','')
                        media_id = message.get('document', {}).get('id', None)
                        sha_256 =  message.get('document', {}).get('sha256', None)
                        lg.info(f"media_id is {media_id}")
                        user_mobile_number =  message.get('from')

                        lg.info(f"sha_256 value is {sha_256}")

                        lg.info(f"user mobile_number is user {user_mobile_number}")
                        db_client_waba_settings=obj_common.get_waba_settings_by_cc_client_number(self.client_number)
                        response_data = db_client_waba_settings.get("response", {})
                        clientname = response_data.get("int_id")
                        clientid=response_data.get("ew_id")
                        # self.client_db.converstaional_data_collection.insert_one({"client_number":self.client_number,"message_response_id":user_message_id,"user_message":user_message_text,"user_mobile_number":user_mobile_number,"timestamp":user_message_timestamp,"session_id":user_message_timestamp,"int_id":clientname,"clientid":clientid,"cost":0.8})
                        # url="https://whatsappservices.engagelybots.ai/testengagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        url = "http://192.168.254.92:9050/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        

                        # url="http://10.2.0.5:9050/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)

                        data = {'contacts': [{'profile': {'name': profile_name}, 'wa_id': str(wa_id)}], 'messages': [{'from': str(wa_id), 'id': str(user_message_id), 'document': {'id': media_id, 'mime_type': 'document/pdf', 'sha256': sha_256, 'status': 'downloaded'}, 'timestamp': user_message_timestamp, 'type': 'document'}]}
                        payload = json.dumps(data)
                        # data = {"contacts": [{"profile": {"name":profile_name }, "wa_id": str(wa_id)}], "messages": [{"from": str(wa_id), "id":str(user_message_id) , "text": {"body":str(user_message_text)}, "timestamp":str(user_message_timestamp), "type": "text"}]}

                        headers = {'X-Wa-Account-Id': '""','Content-Type': 'application/json'}
                        response = requests.request("POST", url, headers=headers, data=payload)
                        lg.info(f"response is {response.text}")



                    elif message.get('type') == 'audio':
                        # current_timestamp = int(datetime.datetime.now().timestamp())
                        clsstatusupdate_obj = ClsStatusUpdate(self.client_number)
                        user_message_id = message.get('id')
                        user_message_timestamp = message.get('timestamp','')
                        media_id = message.get('audio', {}).get('id', None)
                        lg.info(f"media_id is {media_id}")
                        media_url = clsstatusupdate_obj.get_media_url(media_id)

                        lg.info(f"media_url is {media_url}")

                        audio_path = clsstatusupdate_obj.download_audio_file(media_url)

                        lg.info(f"audio_path is {audio_path}")

                        user_message_text = clsstatusupdate_obj.transcribe_audio(audio_path)

                        lg.info(f"user_message_text is {user_message_text}")

                        user_mobile_number =  message.get('from')
                        
                        lg.info(f"user mobile_number is user {user_mobile_number}")
                        db_client_waba_settings=obj_common.get_waba_settings_by_cc_client_number(self.client_number)
                        response_data = db_client_waba_settings.get("response", {})
                        clientname = response_data.get("int_id")
                        clientid=response_data.get("ew_id")
                        self.client_db.converstaional_data_collection.insert_one({"client_number":self.client_number,"message_response_id":user_message_id,"user_message":user_message_text,"user_mobile_number":user_mobile_number,"timestamp":user_message_timestamp,"session_id":user_message_timestamp,"int_id":clientname,"clientid":clientid,"cost":0.8})
                        # url="https://whatsappservices.engagelybots.ai/testengagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        # url = "https://whatsapptest.engagelybots.ai/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        
                        url="http://192.168.254.92:9050/engagely_whatsapp"+"/"+str(clientname)+"/"+str(clientid)
                        
                        data = {"contacts": [{"profile": {"name":profile_name }, "wa_id": str(wa_id)}], "messages": [{"from": str(wa_id), "id":str(user_message_id) , "text": {"body":str(user_message_text)}, "timestamp":str(user_message_timestamp), "type": "text"}]}
                        payload = json.dumps(data)
                        headers = {'X-Wa-Account-Id': '""','Content-Type': 'application/json'}
                        response = requests.request("POST", url, headers=headers, data=payload,timeout=5)
                        lg.info(f"response is {response.text}")
                    

        #lg.info(f"After hitting data_key{data_keys}")
        if statuses_exist:
        
        # if (data_keys[0] == 'statuses'):

            #-----added for external client webhook-------------------
            # celery_task_started = int(time.time()*1000)      
            # lg.critical("data for webhook from webhook functions")                                                                       
            # AsyncSendtoNewWebhook.delay(data, self.client_number)       
            # celery_task_end = int(time.time()*1000)
            # celery_task_time = celery_task_end - celery_task_started
            # print("********",celery_task_time)           
            #-------------------------------end-------------------
            # self.client_db.status_collection.insert_one({"client_number":self.client_number,"status":data})
            # str_status = change['value']['statuses'][0]['status']
            # str_status =  data['statuses'][0]['status']
            # self.client_db.status_collection_updated.insert_one({"client_number":self.client_number,"status":str_status})

         #   lg.info(f"status of message is : {str_status}")
            # if str_status == "sent": ## Removed after version 2.45.1
            #     clsstatusupdate_obj = ClsStatusUpdate(self.client_number)
            #     clsstatusupdate_obj.func_sent_status(data)                              
            if str_status == "delivered":
                # self.client_db.delivered_status_collection.insert_one({"client_number":self.client_number,"data":data})
                clsstatusupdate_obj = ClsStatusUpdate(self.client_number)
                clsstatusupdate_obj.func_delivered_status(data,msg_id,lst_statuses)                   
            if str_status == "read":
                # self.client_db.read_status_collection.insert_one({"client_number":self.client_number,"data":data})
                clsstatusupdate_obj = ClsStatusUpdate(self.client_number)
                clsstatusupdate_obj.func_read_status(data,msg_id,lst_statuses)
            if str_status == "failed":
                # self.client_db.delivered_error.insert_one({"status":data})
                clsstatusupdate_obj = ClsStatusUpdate(self.client_number)
                clsstatusupdate_obj.func_delivered_failed(data,msg_id)

            # err = data['statuses'][0]
            # if 'errors' in err:
            #     # det = data['statuses'][0]['errors'][0]['details']
            #     # det = data['statuses'][0]['errors'][0]
            #     # det = det.get('details','')
            #     # if 'details'=="Queue limit is 1000":
            #     #     func_send_message_request(var_message_body=" ",str_recipient_number= data['statuses'][0]['message']['recipient_id'],str_url_templates=" ",wa_token=" ",client_number=" ",broadcast_id=" ",broadcast_name=" ",data=data)
            #     # else:
            #     self.client_db.delivered_error.insert_one({"status":data})
            #     clsstatusupdate_obj = ClsStatusUpdate(self.client_number)
            #     clsstatusupdate_obj.func_delivered_failed(data)
        
        # For Button Analytics 
        # if (data_keys[0] =='contacts'):
        #     clsbuttonresponse_obj = ClsButtonResponse(self.client_number)
        #     clsbuttonresponse_obj.func_button_response(data)   
        
        # vish added  for message analytics test
        """
        if data:
             clsstatusupdate_obj = ClsStatusUpdate(self.client_number)
             clsstatusupdate_obj.vish_message_test(data)
        """


                
        return "200"

class ClsAppWebhookData():

    def __init__(self,waba_id):
        """ Class for recording button responses from users 
            
        Initialize required object and variables:

        Parameters:
            client_number (str): Mobile Number of WhatsApp 
            db_client_waba_settings (dict): All WABA Settings 
            waba_id (str)  : whats app business account       
        Local Variable:
            client_db_name(str): Fetch and generate DB name for individual client
            ew_db(DB object): Initialize DB
            client_db(DB Object):  Initialize DB
            quality_from_db (str) : quality of client number fetched from database
            quality_from_webhook (str) : quality of client number received on webhook
            Returns:
            All above parameteres and variables
      
        """
        self.waba_id = waba_id
        
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        client_number = self.ew_db.client_waba_settings.find_one({"waba_id":self.waba_id},{"client_number":1,"_id":0})        
        self.client_number = client_number["client_number"]
        db_client_waba_settings = obj_common.get_waba_settings(self.client_number)
        if "error" in db_client_waba_settings:
            return jsonify({"error": "Invalid client number. Please enter appropriate client number"})    
        self.client_db_name = str(db_client_waba_settings["response"]["ew_id"]).lower() + "_" + db_client_waba_settings["response"]["waba_id"]
        self.client_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)
    
    def save_webhook_data(self,data):
        """function for saving updates received on webhook regarding templates status  """
        try:
            if "entry" in data:
                if "changes" in data["entry"][0]:
                    ew_id = self.ew_db.client_waba_settings.find_one({"waba_id":data["entry"][0]["id"]},{"ew_id":1,"_id":0})
                    if "value" in data["entry"][0]["changes"][0]: 
                        data_path = data["entry"][0]["changes"][0]
                        # ew_db.webhook_data.insert_one({"ew_id":ew_id["ew_id"],"field":data_path["field"], "status":data_path["value"]["event"], "message_template_id":data_path["value"][ "message_template_id"], "message_template_language":data_path["value"][ "message_template_language"], "message_template_name":data_path["value"][ "message_template_name"], "reason":data_path["value"][ "reason"], "waba_id":data["entry"][0]["id"],"timestamp":data["entry"][0]["time"],"object":data["object"] })
                        self.ew_db.webhook_data.insert_one({"ew_id":ew_id["ew_id"],"field":data_path["field"],"value":data_path["value"],"waba_id":data["entry"][0]["id"],"timestamp":data["entry"][0]["time"],"object":data["object"]})                 
                        self.client_db.template_mapping.update_one({"wa_response_template_id":str(data_path["value"][ "message_template_id"])}, {"$set":{"template_status":data_path["value"]["event"],"reason":data_path["value"][ "reason"]}})
            return True
        except Exception as e:
            return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
    

    def update_quality_displayname_tier(self,data,lg):
        """function for updates received on webhook regarding quality rating of client number """
        try:            
            if "entry" in data:
                if "changes" in data["entry"][0]:
                    ew_id = self.ew_db.client_waba_settings.find_one({"waba_id":data["entry"][0]["id"]},{"ew_id":1,"_id":0})                   
                    if "value" in data["entry"][0]["changes"][0]: 
                        if data["entry"][0]["changes"][0]["field"]== "phone_number_quality_update":
                            display_phone_number = data["entry"][0]["changes"][0]["value"]["display_phone_number"]
                            quality_from_webhook = data["entry"][0]["changes"][0]["value"]["event"]
                            quality_from_db = self.ew_db.client_profile.find_one({"client_id":ew_id["ew_id"], "client_number":display_phone_number},{"_id":0,"quality_rating":1})["quality_rating"]
                            if quality_from_webhook == "DOWNGRADE":   
                                if quality_from_db == "GREEN":
                                    quality = "YELLOW"
                                elif quality_from_db == "YELLOW":
                                    quality = "RED"
                                elif quality_from_db == "RED":
                                    quality = "RED"                                 
                            elif quality_from_webhook == "UPGRADE":   
                                if quality_from_db == "GREEN":
                                    quality = "GREEN"
                                elif quality_from_db == "YELLOW":
                                    quality = "GREEN"
                                elif quality_from_db == "RED":
                                    quality = "YELLOW"
                            else:
                                quality = quality_from_webhook


                            tier =  data["entry"][0]["changes"][0]["value"]["current_limit"]
                            
                            self.ew_db.client_profile.update_one({"client_id":ew_id["ew_id"], "client_number":display_phone_number},{"$set":{"quality_rating":quality,"message_tier":tier}})
                        
                        if data["entry"][0]["changes"][0]["field"]== "phone_number_name_update": 
                            if data["entry"][0]["changes"][0]["value"]["decision"] == "APPROVED":                                
                                verified_name = data["entry"][0]["changes"][0]["value"]["requested_verified_name"]
                                display_phone_number = data["entry"][0]["changes"][0]["value"]["display_phone_number"]
                                self.ew_db.client_profile.update_one({"client_id":ew_id["ew_id"], "client_number":display_phone_number},{"$set":{"verified_name":verified_name}})
                                self.ew_db.client_business_info.find_one_and_update({"wa_phone_numbers.wa_number": display_phone_number[2:]},{"$set": {"wa_phone_numbers.$.wa_display_name":verified_name}})
            return True
        except Exception as e:
            lg.critical("DB error : " + str(e))
            return jsonify({"id": "1243", "message": "Data Query Error", "description": "", "data": "", "success": False})

     

class ClsCatalogWebhook():

    def __init__(self):
        """ Class for receiving the webhook data for catalog (webhook response for add product to cart ) 
            
        Initialize required object and variables:

        Parameters:
                   
        Local Variable:
      
        """
        pass

    def catalog_webhook(self,data):
        """Send order of KASI client"""
        url = "https://botbuilder.engagely.ai/services_dev/kasi/add_product"
        try:
            requests.request("POST", url, json=data)
        except:
            pass
        return 200
    
