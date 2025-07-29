from wacore.pkg_app_webhook.mod_app_webhook_functions import ClsAppWebhook
from flask import Flask,Blueprint, jsonify, request
from walogger.walogger import WaLogger
from wacore.pkg_webhook.mod_webhook_functions import ClsWebhook,ClsAppWebhookData,ClsCatalogWebhook
import json
import ast

from waapi.celery_worker3 import async_updateanalytics

app_webhook = Blueprint('app_webhook', __name__,url_prefix='/waapi/appwh')

# Initialize logger with name that we want or any other
obj_log = WaLogger('pktmp')
lg = obj_log.get_logger()

@app_webhook.route('/appwebhook',methods=["POST","GET"])
def appwebhook():
    """ Used to Update Template status,Account,Phones and more """
    print("Print insdie dev")
    lg.critical(f"inside webhook dev")
    try:
        data = json.loads(request.data)
        lg.info(f"webhook data is {data}")
        # from pymongo import MongoClient
        # url = "mongodb://20.204.130.168:21073/" 
        # client = MongoClient(url)
        # # client_db = "ew51"
        # ew_id = "ew51"
        # client_db = client[ew_id]
        # client_db.webhook_data.insert_one({"webhook_data":data})
        event_type=data["entry"][0]["changes"][0]["field"]
        lg.info(f"Event={event_type} Received")
        # Check event_type

        if event_type=="messages":
           try: 
                client_number = data["entry"][0]["changes"][0]["value"]["metadata"]["display_phone_number"]
                lg.info(f"client_number is {client_number}")
                for entry in data.get('entry', []): 
                    for change in entry.get('changes', []):  
                        if 'value' in change and isinstance(change['value'], dict):
                            if 'statuses' in change['value']:
                                tasksd = async_updateanalytics.apply_async(args=(client_number,data))
                                result_job = async_updateanalytics.AsyncResult(tasksd.id)
                                lg.info(f"task is taskid is {tasksd.id} , task status is  Task Status: {result_job.status} ,  args data is {client_number} , {data}")
                                result = jsonify({"success": "True", "msg": "Async request started","taskid":tasksd.id}), 200
                                lg.info("dev3")
                                return result
                            elif 'messages' in change['value']:
                                messages = change['value']['messages']
                                contacts = change['value']['contacts']
                        for message in messages:
                            if message.get('type') == 'button':
                                try:
                                     button_payload = message.get('button', {}).get('payload', None)
                                    #  button_text = message.get('button', {}).get('text', None)
                                    #  button_id = message['context']['id']
                                     lg.info(f"Button_payload is {button_payload} type is {type(button_payload)}")
                                     button_payload = ast.literal_eval(button_payload)
                                     len_journey = len(button_payload.get('J', ''))
                                     lg.info(f"length of journey is {len_journey}")
                                     if 'J' in button_payload and len_journey != 0:
                                            obj_webhook = ClsWebhook(client_number)
                                            obj_webhook.func_webhook(data)
                                            return '{"success": "OK"}', 200
                                     else:
                                            tasksd = async_updateanalytics.apply_async(args=(client_number,data))
                                            result_job = async_updateanalytics.AsyncResult(tasksd.id)
                                            lg.info(f"task is taskid is {tasksd.id} , task status is  Task Status: {result_job.status} ,  args data is {client_number} , {data}")
                                            result = jsonify({"success": "True", "msg": "Async request started","taskid":tasksd.id}), 200
                                            lg.info("dev3")
                                            return '{"success": "OK"}', 200
                                except Exception as e:
                                    lg.info(f"Exception is {e}")
                                    result = " "
                                    return result

                                            
                            elif message.get('type') == 'text':
                                  lg.info(f"sending to wacore code")
                                  obj_webhook = ClsWebhook(client_number)
                                  obj_webhook.func_webhook(data)
                                  return '{"success": "OK"}', 200
                            
                            elif message.get('type') == 'interactive':
                                obj_webhook = ClsWebhook(client_number)
                                obj_webhook.func_webhook(data)
                                return '{"success": "OK"}', 200
                            
                            elif message.get('type') == "audio":
                                 obj_webhook = ClsWebhook(client_number)
                                 obj_webhook.func_webhook(data)
                                 return '{"success": "OK"}', 200
                            
                            elif message.get('type') == "image":
                                 obj_webhook = ClsWebhook(client_number)
                                 obj_webhook.func_webhook(data)
                                 return '{"success": "OK"}', 200
                            
                            elif message.get('type') == "video":
                                 obj_webhook = ClsWebhook(client_number)
                                 obj_webhook.func_webhook(data)
                                 return '{"success": "OK"}', 200
                            
                            elif message.get('type') == "document":
                                 obj_webhook = ClsWebhook(client_number)
                                 obj_webhook.func_webhook(data)
                                 return '{"success": "OK"}', 200
           
           
                return '{"success": "OK"}', 200                                
           except Exception as e:
               lg.critical("analytics celery failed: " + str(e))
               return jsonify({"error": {"id": "1231", "message": "Something went wrong in payload or result"}, "success": "False"})
               

        else:

            if event_type=="message_template_status_update":
                waba_id=data["entry"][0]["id"]
                appwh_obj = ClsAppWebhook(waba_id)
                appwh_obj.fn_update_temp_status(data)
            elif event_type=="phone_number_quality_update":
                waba_id=data["entry"][0]["id"]
                appwh_obj = ClsAppWebhook(waba_id)
                appwh_obj.fn_update_phone_quality_tier(data)
            else:
                lg.info("No action taken, try saving.")
                waba_id=data["entry"][0]["id"]
                appwh_obj = ClsAppWebhook(waba_id)
                appwh_obj.fn_save_data(data)
            return '{"success": "OK"}', 200
    except Exception:
        hub_challenge = str(request.args.get("hub.challenge"))
        hub_verify_token = str(request.args.get("hub.verify_token"))
        result = '{"hub.verify_token":"'+ hub_verify_token +'", "hub.challenge": "'+hub_challenge+'"}'
        challenge_result = '{"hub.challenge": "'+hub_challenge+'"}'
        lg.info(result)
        lg.info(hub_verify_token)
        return hub_challenge, 200
        lg.info(f"Failed with some issue")
        return '{"except success": "OK"}', 200
