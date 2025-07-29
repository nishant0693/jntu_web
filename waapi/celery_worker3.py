import os
from celery import Celery
from dotenv import load_dotenv 
from flask import Blueprint, jsonify, request

# Import custom packages
#from wacore.pkg_template_broadcast.mod_celery_broadcast2 import func_send_message_request
from wacore.pkg_webhook.mod_webhook_functions import ClsWebhook,ClsAppWebhookData,ClsCatalogWebhook


load_dotenv(".env")

celery = Celery(__name__)

celery.conf.update(
    result_expires=15,
    task_acks_late=True,
    broker_url = os.environ.get("CELERY_BROKER_URL"),
    result_backend = os.environ.get("CELERY_RESULT_BACKEND"),
    task_serializer='json',
    task_max_size=1024 * 1024 * 10
    
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
@celery.task(name="async_updateanalytics_pending",queue="async_updateanalytics_pending")
def async_updateanalytics_pending(client_number,data):
        try:
            obj_webhook = ClsWebhook(client_number)
            obj_webhook.func_webhook(data)
            result = {"success": "True", "msg": "Async request started"}
            return result
        except Exception as e:
            return jsonify({"error": {"id": "1231", "message": "Something went wrong in payload or result"}, "success": False})
#-------------------------------------------------------
