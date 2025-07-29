import os
from celery import Celery
from dotenv import load_dotenv 
from flask import Blueprint, jsonify, request
import datetime
import requests
import json


#Import custom packages
from wacore.pkg_db_connect.mod_db_connection import ClsMongoDBInit
from wacore.pkg_extras.mod_common import ClsCommon
from wacore.auth.mod_login_functions import phone_access_required
from walogger.walogger import WaLogger

obj_log = WaLogger('pkanalytics')
lg = obj_log.get_logger()

# Import custom packages
#from wacore.pkg_template_broadcast.mod_celery_broadcast2 import func_send_message_request
from wacore.pkg_webhook.mod_webhook_functions import ClsWebhook,ClsAppWebhookData,ClsCatalogWebhook

load_dotenv(".env")

'''
celery = Celery(__name__)

celery.conf.update(
    result_expires=15,
    task_acks_late=True,
    broker_url = os.environ.get("CELERY_BROKER_URL"),
    result_backend = os.environ.get("CELERY_RESULT_BACKEND")
    
)


#-------------------------------------------------------new single msg send task
@celery.task(name="updateanalytics",queue="updateanalytics")
def async_updateanalytics(client_number,data):
        try:
            obj_webhook = ClsWebhook(client_number)
            obj_webhook.func_webhook(data)
            result = {"success": "True", "msg": "Async request started"}
            return result
        except Exception as e:
            return jsonify({"error": {"id": "1231", "message": "Something went wrong in payload or result"}, "success": False})
#-------------------------------------------------------

'''