"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains schemas of DMP webhook package only
    * Description: All the DMP webhook routes persent here
    
"""

from flask import Blueprint, jsonify, request
import json

# Import custom packages
from wacore.pkg_webhook.mod_webhook_functions import ClsWebhook,ClsAppWebhookData,ClsCatalogWebhook
from walogger.walogger import WaLogger
from waapi.celery_worker3 import async_updateanalytics
app_webhook = Blueprint('app_dmp_webhook', __name__,url_prefix='/waapi/webhook')

# Initialize logger with name that we want or any other
obj_log = WaLogger('pkwbhk')
lg = obj_log.get_logger()


class ClsWebhookRoutes():
    """ Class of route where webhhook routes where webhook data is received. """
    
    
    def __init__(self):
        """
        Initialize required object and variables
        """
        pass

    @app_webhook.route('/dmpservice_webhook',methods=["POST"]) 
    def func_save_webhook_data():
        """ Method called for webhook data """
        data = json.loads(request.data)
        # print(data)
        # print("-"*55)
        client_number = request.headers["X-Wa-Account-Id"] 
        lg.info(f"Inside web hook api method {client_number},{data}")
        try:
            tasksd = async_updateanalytics.apply_async(args=(client_number,data))
            result_job = async_updateanalytics.AsyncResult(tasksd.id)
            lg.info(f"task is taskid is {tasksd.id} , task status is  Task Status: {result_job.status} ,  args data is {client_number} , {data}")
            result = jsonify({"success": "True", "msg": "Async request started","taskid":tasksd.id}), 200
            lg.info("dev3")
            return result
        except Exception as e:
            lg.critical("analytics celery failed: " + str(e))
            return jsonify({"error": {"id": "1231", "message": "Something went wrong in payload or result"}, "success": "False"})

        '''
        try:
            obj_webhook = ClsWebhook(client_number)
            lg.info(f"dev")
            obj_webhook.func_webhook(data)
            lg.info("dev2")
            result = jsonify({"success": True, "msg": "Async request started"}), 200
            lg.info("dev3")
            return result
        except Exception as e:
            lg.critical("list_of_numbers API failed: " + str(e))
            return jsonify({"error": {"id": "1231", "message": "Something went wrong in payload or result"}, "success": False})
        '''
        
    @app_webhook.route('/appwebhook',methods=["POST","GET"])
    def save_webhook_data():
        """function for updates received on webhook regarding templates stattus and quality rating of client number """
        # try:
        json_object = json.loads(request.data)
        json_formatted_str = json.dumps(json_object, indent=2)
        data=json_object
        waba_id = data["entry"][0]["id"]
        appwh_obj = ClsAppWebhookData(waba_id)
        save_status = appwh_obj.save_webhook_data(data)
        if save_status == True:
            pass             
        try:               
            if data["entry"][0]["changes"][0]["field"] == "phone_number_quality_update" or  data["entry"][0]["changes"][0]["field"]== "phone_number_name_update":
                appwh_obj.update_quality_displayname_tier(data,lg)
        except Exception as e:
            lg.critical("appwebhook API failed: " + str(e))
            return jsonify({"id": "1241", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})

        return jsonify({"id": "1242", "message": "app webhook data processed", "description": "", "data": "", "success": True}), 200

        
    @app_webhook.route('/order_webhook',methods=["POST"]) 
    def save_order_data():
        json_object = json.loads(request.data)
        data = json_object
        # client_number = request.headers["X-Wa-Account-Id"]
        ClsCatalogWebhook().catalog_webhook(data)
        return jsonify({"id": "1251", "message": "Webhook data processed", "description": "", "data": "", "success": True})            
