import os
import time

from celery import Celery
from dotenv import load_dotenv
import requests

from walogger.walogger import WaLogger
obj_log = WaLogger('pkgdmp')
lg = obj_log.get_logger()
# Import custom packages
from wacore.pkg_template_broadcast.mod_celery_broadcast import ClsCeleryBroadcast
# from wacore.pkg_template_broadcast.mod_celery_broadcast2 import func_send_message_request
from wacore.pkg_db_connect.mod_db_connection import ClsMongoDBInit

ew_db = ClsMongoDBInit.get_ew_db_client()

load_dotenv(".env")

celery = Celery(__name__)

celery.conf.update(
    result_expires=15,
    task_acks_late=True,
    broker_url = os.environ.get("CELERY_BROKER_URL"),
    result_backend = os.environ.get("CELERY_RESULT_BACKEND"),
    task_serializer='json'
)

@celery.task(name="create_task")
def create_task(b, c):
    time.sleep(30)
    return b + c

@celery.task(name="main_broadcast_tasks")
def async_broadcast_template_messages(lst_columns,excel_file,email_id,consen_header,**kwargs):
    """
    Accepth the celery broadcast job after excel verification

    Parameters: [**kwargs]
        {ew_id,client_number,template_name,broadcast_id,broadcast_name,client_db_name}
    
    Local Variables:
        None
    Returns:
        Successful Broadcast Completion Result
    """
    obj_celery_broadcast = ClsCeleryBroadcast(lst_columns,excel_file,kwargs)
    var_contact_status = obj_celery_broadcast.fun_get_contact_status(email_id,consen_header)
    return "CeleryJob Finished"


# #-------------------------------------------------------new
# @celery.task(name="broadcast_tasks2")
# def async_broadcast_single_template(var_message_body,str_recipient_number,str_url_templates,wa_token,client_number,broadcast_id,broadcast_name):
#     var_contact_status = func_send_message_request(var_message_body,str_recipient_number,str_url_templates,wa_token,client_number,broadcast_id,broadcast_name)
#     return "Single MSG Finished"
# #-------------------------------------------------------


@celery.task(name="/AsyncSendtoNewWebhook")
def AsyncSendtoNewWebhook(data,client_number):   
    db_client_external_webhook = ew_db.client_external_webhook.find_one({"client_number":(client_number)},{"_id":0})
    filtered_data = {"statuses":[{"message_id":data["statuses"][0]["id"],"recipient_id": data["statuses"][0]["recipient_id"],
    "status": data["statuses"][0]["status"],
    "timestamp": data["statuses"][0]["timestamp"],
    "type": data["statuses"][0]["type"]}]}
        
    if db_client_external_webhook != None:
        is_active = db_client_external_webhook["is_active"]
        if is_active == True:    
            url = db_client_external_webhook["client_webhook"]           
            # lg.info("data for webhook " + data + "for " + client_number)
            try:
                requests.request("POST", url, json=filtered_data)
            except:
                lg.info("unable to send data to external webhook") 
                return {"message":"webhook data sending failed for", "message_id":data["statuses"][0]["id"]}
   
    return {"message":"webhook data sent on clients webhook", "message_id":data["statuses"][0]["id"]}



