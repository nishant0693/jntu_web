"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains schemas of template package only
    * Description: All the template routes present here
"""

from flask import Blueprint, jsonify, request
from flask_cors import CORS, cross_origin
from flask_jwt_extended import get_jwt
import datetime
import pandas as pd
from flask import json
import os
# from datetime import datetime

# Import custom packages
from wacore.pkg_template_broadcast.mod_template_functions import ClsBroadcast, ClsBroadcastoperations
from wacore.pkg_template_operations.mod_template_functions import ClsTemplateOperations,ClsTemplatestatus
from wacore.pkg_extras.mod_common import ClsCommon
from wacore.auth.mod_login_functions import token_required
from walogger.walogger import WaLogger



app_templates = Blueprint('app_templates', __name__,url_prefix='/waapi/template')

# Initialize logger with name that we want or any other
obj_log = WaLogger('pktmp')
lg = obj_log.get_logger()


class ClsTemplateManagement():
    """ Class called for template routes"""    

    def __init__(self):
        """ Create or initialize object and variables """
        pass


    #unused
    @app_templates.route("/template_list", methods=["GET"])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_get_template_list():
        """ Method called to get list of template names """
        client_id = request.headers['client_id']
        flag_data = request.headers['template_data']
        try:
            obj_template = ClsTemplateOperations("")
            result = obj_template.func_get_all_templates(client_id,flag_data,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "template_list API failed: " + str(e))
            return jsonify({"id": "1041", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_templates.route('/create_header_handle',methods = ['POST'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_header_handle():
        """ Method called to create handle handle """
        claims = get_jwt()
        client_id = claims["ew_id"]
        file = request.files["file"]
        file_type = request.form["file_type"]
        client_number = request.form["mobile_number"]
        lg.info(f"client_number is {client_number}")
        obj_common = ClsCommon()
        try:
            db_client_waba_settings = obj_common.get_waba_settings_by_cc_client_number(client_number)
        except Exception as e:
            lg.critical(f"ew_id={client_id} | DB error - Failed to fetch WABA phone information: {str(e)}")    
            return jsonify({"id": "8643", "message": "Failed to fetch phone information.", "description": "Invalid client_number.", "success": False})

        if "error" in db_client_waba_settings:
            lg.critical(f"ew_id={client_id} | DB error - client_waba_settings for client number is None")    
            return jsonify({"id": "8644", "message": "Failed to get phone information.", "description": "Invalid client_number.", "success": False})

        try:
            obj_broadcast = ClsBroadcastoperations(db_client_waba_settings)
            phonenumberid = db_client_waba_settings['response']['Phone-Number-ID']
            
            file_path = os.path.join(os.getcwd(), file.filename)
            file.save(file_path)

            result, status_code = obj_broadcast.get_media_id(client_id, file_path, lg, phonenumberid)
        
            if status_code == 200:
                response_data = result.get_json()
                if "id" in response_data:
                 media_id = response_data["id"]
                 lg.info(f"Media ID is {media_id}")
                else:
                   lg.error(f"Missing 'id' in API response: {response_data}")
            else:
              lg.error(f"Media upload failed with status {status_code}: {result.get_json()}")

            # return result

        except Exception as e:
            if os.path.exists(file_path):
                os.remove(file_path)
            lg.critical(f"ew_id={client_id} | upload_media_id function failed: {str(e)}")
            return jsonify({"id": "1193", "message": "Invalid data", "description": "Something went wrong.", "success": False})

        try:
            obj_template = ClsTemplateOperations(db_client_waba_settings)
            result = obj_template.func_create_header_handle(file,file_type,client_id,lg, media_id)
            # return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "create_header_handle API failed: " + str(e))
            return jsonify({"id": "1051", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})
        
        return result



    @app_templates.route('/create_template',methods = ['POST'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_create_template():
        """ Method called to create template """
        client_number = request.args.get("client_number")
        flow_name = request.json.get("flow_name", " ")
        lg.info(f"selected flow name is {flow_name}")
        lg.info(f"client_number is {client_number}")
        claims = get_jwt()
        client_id = claims["ew_id"]
        email_id = claims["email_id"]
        bot_id = claims["bot_id"]
        data = request.json
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_cc_client_number(client_number)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for client number: " + str(e))    
            return jsonify({"id": "1061", "message": "Invalid credentials", "description": "Invalid client_number. Client number is not linked with internal DB", "data": "", "success": False})        
        if "error" in db_client_waba_settings or db_client_waba_settings["response"] == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for client number : None")    
            return jsonify({"id": "1062", "message": "Invalid credentials", "description": "Invalid client number. Client number is not linked with internal DB", "data": "", "success": False})        
        try:
            obj_template = ClsTemplateOperations(db_client_waba_settings)
            lg.info(f"db_client_waba settings is {db_client_waba_settings} ")
            base_url = db_client_waba_settings['response']['url']
            waba_id = db_client_waba_settings['response']['waba_id']
            access_token = db_client_waba_settings['response']['access_token']
            result =  obj_template.func_message_template_operation(client_number,client_id,email_id,bot_id,data,lg,flow_name,base_url,waba_id,access_token)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "create_template API failed: " + str(e))
            return jsonify({"id": "1063", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})

    
    #unused
    @app_templates.route("/get_template", methods=["GET"])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_get_template():
        """ Method called to get template by name """
        client_id = request.headers['client_id']
        template_name = request.headers['template_name']
        obj_template = ClsTemplateOperations("")
        try:
            result =  obj_template.func_get_template_data(client_id,template_name,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "get_template API failed: " + str(e))
            return jsonify({"id": "1081", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    #unused
    @app_templates.route("/check_template_uniqueness", methods=["GET"])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_check_template_uniqueness():
        """ Method called to check uniqueness of template """
        client_id = request.headers['client_id']
        template_name = request.headers['template_name']
        obj_template = ClsTemplateOperations("")
        try:
            result = obj_template.template_uniqueness(client_id,template_name,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "check_template_uniqueness API failed: " + str(e))
            return jsonify({"id": "1091", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    #unused
    @app_templates.route("/send_language_name_catagory", methods=["GET"])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_get_language_catagory():
        """ Method called to send language and catagory """
        claims = get_jwt()
        client_id = claims["ew_id"]
        try:    
            obj_template = ClsTemplateOperations("")
            result = obj_template.func_list_language_catagory(lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "check_template_uniqueness API failed: " + str(e))
            return jsonify({"id": "1101", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})

    
    @app_templates.route('/delete_template',methods = ['POST'])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_message_delete():
        """ Method called to delete template """
        claims = get_jwt()
        client_id = claims["ew_id"]
        bot_id = claims["bot_id"]
        email_id = claims["email_id"]
        name = request.json["template_name"]
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : " + str(e))    
            return jsonify({"id": "1111", "message": "Invalid credentials", "description": "Invalid boyt id. Bot id is not linked with internal DB", "data": "", "success": False})        
        if "error" in db_client_waba_settings or db_client_waba_settings["response"] == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : None")    
            return jsonify({"id": "1112", "message": "Invalid credentials", "description": "Invalid Bot id. Bot id is not linked with internal DB", "data": "", "success": False})        
        try:
            obj_template = ClsTemplateOperations(db_client_waba_settings)
            result =  obj_template.func_delete_template(client_id,name,email_id,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "delete_template API failed: " + str(e))
            return jsonify({"id": "1113", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_templates.route('/create_excelsheet',methods = ['POST'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_create_excelsheet():
        """ Method called to create excelsheet for specific template """
        claims = get_jwt()
        client_id = claims["ew_id"]
        bot_id = claims["bot_id"]
        email_id = claims["email_id"]
        data = request.json
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : " + str(e))    
            return jsonify({"id": "1121", "message": "Invalid credentials", "description": "Invalid client_number. Client number is not linked with internal DB", "data": "", "success": False})        
        if "error" in db_client_waba_settings or db_client_waba_settings["response"] == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : None")    
            return jsonify({"id": "1122", "message": "Invalid credentials", "description": "Invalid client_number. Client number is not linked with internal DB", "data": "", "success": False})        
        try:
            obj_template = ClsTemplateOperations(db_client_waba_settings)
            result = obj_template.func_create_excelsheet(data,client_id,email_id,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "create_excelsheet API failed: " + str(e))
            return jsonify({"id": "1123", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_templates.route('/get_template_names',methods = ['GET'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_get_template_names():
        """ Method called to fetch template list from engagely db """
        claims = get_jwt()
        client_id = claims["ew_id"]
        email_id = claims["email_id"]
        bot_id = claims["bot_id"]
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : " + str(e))    
            return jsonify({"id": "1131", "message": "Invalid credentials", "description": "Invalid bot id. Bot id is not linked with internal DB", "data": "", "success": False})        
        if "error" in db_client_waba_settings or db_client_waba_settings["response"] == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : None")    
            return jsonify({"id": "1132", "message": "Invalid credentials", "description": "Invalid bot id. Bot id is not linked with internal DB", "data": "", "success": False})        
        try:
            obj_template = ClsTemplateOperations(db_client_waba_settings)
            result =  obj_template.func_get_template_names(client_id,email_id,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "get_template_names API failed: " + str(e))
            return jsonify({"id": "1133", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_templates.route('/get_template_by_name',methods = ['POST'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_get_template_name_data():
        """ Method called to fetch template data by name from engagely db """
        claims = get_jwt()
        client_id = claims["ew_id"]
        email_id = claims["email_id"]
        bot_id = claims["bot_id"]
        data = request.json
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : " + str(e))    
            return jsonify({"id": "1331", "message": "Invalid credentials", "description": "Invalid bot id. Bot id is not linked with internal DB", "data": "", "success": False})        
        if "error" in db_client_waba_settings or db_client_waba_settings["response"] == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : None")    
            return jsonify({"id": "1332", "message": "Invalid credentials", "description": "Invalid bot id. Bot id is not linked with internal DB", "data": "", "success": False})        
        try:
            obj_template = ClsTemplateOperations(db_client_waba_settings)
            result =  obj_template.func_get_template_by_name(client_id,email_id,data,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "get_template_names API failed: " + str(e))
            return jsonify({"id": "1333", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_templates.route('/get_template_by_name_for_edit_template',methods = ['POST'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_get_template_name_data_edit():
        """ Method called to fetch template data by name from engagely db """
        claims = get_jwt()
        client_id = claims["ew_id"]
        email_id = claims["email_id"]
        bot_id = claims["bot_id"]
        data = request.json
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : " + str(e))    
            return jsonify({"id": "1331", "message": "Invalid credentials", "description": "Invalid bot id. Bot id is not linked with internal DB", "data": "", "success": False})        
        if "error" in db_client_waba_settings or db_client_waba_settings["response"] == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : None")    
            return jsonify({"id": "1332", "message": "Invalid credentials", "description": "Invalid bot id. Bot id is not linked with internal DB", "data": "", "success": False})        
        try:
            obj_template = ClsTemplateOperations(db_client_waba_settings)
            result =  obj_template.func_get_template_by_name_for_edit_template(client_id,email_id,data,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "get_template_names API failed: " + str(e))
            return jsonify({"id": "1333", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_templates.route('/whatsapp_journey_list',methods = ['GET'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def get_journey():
        """ Method called to get whatsapp journey list from engagely db """
        claims = get_jwt()
        client_id = claims["ew_id"]
        client_number = request.args.get("client_number")
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_cc_client_number(client_number)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for client number : " + str(e))    
            return jsonify({"id": "1141", "message": "Invalid credentials", "description": "Invalid client_number. Client number is not linked with internal DB", "data": "", "success": False})        
        if "error" in db_client_waba_settings or db_client_waba_settings["response"] == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for client number : None")    
            return jsonify({"id": "1142", "message": "Invalid credentials", "description": "Invalid client_number. Client number is not linked with internal DB", "data": "", "success": False})        
        
        try:
            obj_template = ClsTemplateOperations(db_client_waba_settings)
            result =  obj_template.func_list_wa_journey()
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "whatsapp_journey_list API failed: " + str(e))
            return jsonify({"id": "1143", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})
        

    @app_templates.route('/quick_reply_type',methods = ['GET'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def get_quick_reply_type():
        """ Method called to get quick reply type from engagely db """
        claims = get_jwt()
        client_id = claims["ew_id"]
        try:
            obj_template = ClsTemplateOperations(None)
            result =  obj_template.func_quick_reply_type(client_id,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "quick_reply_type API failed: " + str(e))
            return jsonify({"id": "1151", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_templates.route('/change_template_access',methods = ['POST'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def change_template_access():
        """ Method called to change template access status from engagely db """
        claims = get_jwt()
        bot_id = claims["bot_id"]
        client_id = claims["ew_id"]
        email_id = claims["email_id"]
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : " + str(e))    
            return jsonify({"id": "1161", "message": "Invalid credentials", "description": "Invalid bot_id. Bot id is not linked with internal DB", "data": "", "success": False})        
        if "error" in db_client_waba_settings or db_client_waba_settings["response"] == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : None")    
            return jsonify({"id": "1162", "message": "Invalid credentials", "description": "Invalid not id. Bot id is not linked with internal DB", "data": "", "success": False})        
        
        try:
            data = request.json
            obj_template = ClsTemplateOperations(db_client_waba_settings)
            result =  obj_template.func_change_template_access(client_id,email_id,data,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "change_template_access API failed: " + str(e))
            return jsonify({"id": "1163", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    # ------------------------------------- Broadcast Routes -------------------------------------

    @app_templates.route('/send_excel_templates',methods = ['POST'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def notification_with_excel():
        """ Method called to broadcast excel sheet for templates"""
        consen_header = json.loads(json.dumps({**request.headers}))
        lg.info(f'header for this api is {consen_header}')
        claims = get_jwt()
        email_id = claims["email_id"]
        client_id = claims["ew_id"]
        excel_file = request.files["excel_file"]
        # df_init = pd.read_excel(excel_file)
        # lg.info(f"head and tail values  {df_init.head()} {df_init.tail()}")
        # col_name = df_init.columns[3]
        # if df_init [col_name].isna().any():
        #     lg.info("nan values found")
        # else:
        #     lg.info("nan values not found")
        broadcast_name = request.form["broadcast_name"]
        template_name = request.form["template_name"]
        client_number = request.form["client_number"]

        
        # Get clinet WABA settings 
        obj_common = ClsCommon()
        try:
            db_client_waba_settings = obj_common.get_waba_settings_by_cc_client_number(client_number)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - Failed to fetch WABA phone information:" + str(e))    
            return jsonify({"id": "1171", "message": "Failed to fetch phone information. Please, try again.", "description": "Invalid client_number. Client number is not linked with internal DB", "data": "", "success": False})
        
        if "error" in db_client_waba_settings:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for client number : None")    
            return jsonify({"id": "1172", "message": "Failed to get phone information. Please, try again.", "description": "Invalid client_number. Client number is not linked with internal DB", "data": "", "success": False})        
        
        # Init and start broadcast by submitting to celery
        try:
            obj_broadcast_var_status = False
            obj_broadcast = ClsBroadcast(client_number,email_id,db_client_waba_settings,broadcast_name,template_name,excel_file,consen_header)
            # obj_broadcast = ClsBroadcast(client_number,email_id,db_client_waba_settings,broadcast_name,template_name,excel_file,df_init)
            obj_broadcast_var_status = True
            lg.info(f"Time before calling the init_excel_broadcast function is {datetime.datetime.utcnow()}")
            result = obj_broadcast.func_init_excel_broadcast(email_id,client_id,lg,consen_header)
            # result = obj_broadcast.func_init_excel_broadcast(email_id,client_id,lg,df_init)
            lg.info(f"Time After calling the init_excel_broadcast function is {datetime.datetime.utcnow()}")
            return result
        except Exception as e:
            if obj_broadcast_var_status == False:
                lg.critical("ew_id=" + str(client_id) + " | " + "Failed to setup broadcast. Please, try again." + str(e))    
                return jsonify({"id": "1172", "message": "Failed to setup broadcast. Please, try again.", "description": "Invalid data received.", "data": "", "success": False})        
            else:
                lg.critical("ew_id=" + str(client_id) + " | " + "Broadcast Failed. Please,check broadcast log information." + str(e))    
                return jsonify({"id": "1172", "message": "Broadcast Failed. Please,check broadcast log information.", "description": "Invalid data received.", "data": "", "success": False}) 
        


    @app_templates.route('/get_broadcast_list',methods = ['POST'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_get_broadcast_list():
        """ Route to upload Excel and send Templates one by one """
        claims = get_jwt()
        client_id = claims["ew_id"]
        bot_id = claims["bot_id"]
        email_id = claims["email_id"]
        
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : " + str(e))    
            return jsonify({"id": "1181", "message": "Invalid credentials", "description": "Invalid client number", "data": "", "success": False})        
        if "error" in db_client_waba_settings or db_client_waba_settings["response"] == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : None")    
            return jsonify({"id": "1182", "message": "Invalid credentials", "description": "Invalid client number. Please use correct client number", "data": "", "success": False})
        
        try:
            page_size = request.json["page_size"]
            page_num = request.json["page_num"]
            int_from_timestamp = request.json["start_time"]
            int_to_timestamp = request.json["end_time"]
            int_to_timestamp = int(int_to_timestamp) + (5 * 3600) + (30 * 60)
            search_pattern = request.json["search_value"]
            # search_pattern = ""
            obj_broadcast = ClsBroadcastoperations(db_client_waba_settings)
            result =  obj_broadcast.func_get_broadcast_list_function(client_id,email_id,page_size,page_num,int_from_timestamp,int_to_timestamp,lg,search_pattern)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "get_broadcast_list API failed: " + str(e))
            return jsonify({"id": "1183", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})
    
    
    @app_templates.route('/delete_broadcast_item',methods = ['POST'])
    @token_required
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def func_delete_broadcast_item():
        """ Route to upload Excel and send Templates one by one """
        claims = get_jwt()
        client_id = claims["ew_id"]
        broadcast_id = request.json["broadcast_id"]
        bot_id = claims["bot_id"]
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : " + str(e))    
            return jsonify({"id": "1191", "message": "Invalid credetials", "description": "Invalid client number", "data": "", "success": False})        
        if "error" in db_client_waba_settings or db_client_waba_settings["response"] == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : None")    
            return jsonify({"id": "1192", "message": "Invalid credentials", "description": "Invalid client number. Please use correct client number", "data": "", "success": False})
        
        try:
            obj_broadcast = ClsBroadcastoperations(db_client_waba_settings)
            result =  obj_broadcast.func_delete_broadcast_function(client_id,broadcast_id,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "delete_broadcast_item API failed: " + str(e))
            return jsonify({"id": "1193", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_templates.route('/update_template_status',methods=["POST"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    def update_template_status():  
        data = ClsTemplatestatus()
        result = data.updated_template_status(lg)
        return result
