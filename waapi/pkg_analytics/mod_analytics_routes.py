from flask import Blueprint, jsonify, request
from flask_cors import CORS, cross_origin
from flask_jwt_extended import get_jwt


import json


#Import custom routes
from wacore.pkg_analytics.mod_analytics_function import ClsBroadcastAnalysis, ClsAnalytics, ClsDmpServiceAnalytics
from wacore.pkg_extras.mod_common import ClsCommon
from wacore.auth.mod_login_functions import token_required
from walogger.walogger import WaLogger

app_analytics = Blueprint('analytics', __name__,url_prefix='/waapi/analytics')

# Initialize logger with name that we want or any other
obj_log = WaLogger('pkanalytcs')
lg = obj_log.get_logger()

class ClsDAnalyticsRoutes():
    """ Class for analytics routes """
    
    def __init__(self):
        """ 
        Initialize required object and variables
        """
        pass

    @app_analytics.route('/list_of_numbers',methods=["GET"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_list_of_numbers():
        """ Method called to get list of numbers """
        claims = get_jwt()
        client_id = claims["ew_id"]
        bot_id = claims["bot_id"]
        accessible_phones = claims["accessible_phones"]
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : " + str(e))    
            return jsonify({"id": "1201", "message": "Invalid credentials", "description": "Invalid bot id. Bot id is not linked with internal DB", "data": "", "success": False})        
        if "error" in db_client_waba_settings:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings for bot id : None")    
            return jsonify({"id": "1202", "message": "Invalid credentials", "description": "Invalid bot id. Bot id is not linked with internal DB", "data": "", "success": False})    

        try:
            obj_broadcast_analytics = ClsAnalytics(db_client_waba_settings)
            result = obj_broadcast_analytics.func_list_of_numbers(client_id,accessible_phones,lg)
            return result  
        except Exception as e:
            # lg.critical("ew_id=" + str(client_id) + " | " + "list_of_numbers API failed: " + str(e))
            return jsonify({"id": "1203", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_analytics.route('/excel_basic_analytics',methods=["POST"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def excel_basic_analytics():
        """Fuction called to get analytics of broadcasts in given time range"""
        claims = get_jwt()
        client_id = claims["ew_id"]
        email_id = claims["email_id"]
        print(f"thisisclais{claims}")
        lg.critical(f"thisisclais{claims}")
        try:
            all = request.json["all"]
            if all == False:
                client_number = request.json["client_number"]                
            else:
                client_number = claims["accessible_phones"][0]
                # client_number="966592628460"
            int_from_timestamp = request.json["start_time"]
            int_to_timestamp = request.json["end_time"]
            int_to_timestamp = int(int_to_timestamp) + (5 * 3600) + (30 * 60)
            page_size = request.json["page_size"]
            page_num = request.json["page_num"]
            str_template_name = request.json["template_name"]
            all_template = request.json["all_template"]
            obj_broadcast_analytics = ClsBroadcastAnalysis(client_number)
            result = obj_broadcast_analytics.func_broadcast_basic_analysis(client_number,client_id,all,int_from_timestamp,int_to_timestamp,email_id,page_size,page_num,str_template_name,all_template,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "excel_basic_analytics API failed: " + str(e))
            return jsonify({"id": "1211", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_analytics.route('/analytics_excel_download',methods=["POST"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def analytics_excel_download():
        """Function called to download analytics of particular broadcat in excel"""
        claims = get_jwt()
        client_id = claims["ew_id"]
        client_number = request.args.get("client_number")
        broadcast_id = request.json["broadcast_id"]
        try:
            obj_broadcast_analytics = ClsBroadcastAnalysis(client_number)
            lg.info("dev")
            result =obj_broadcast_analytics.func_broadcast_excel_download(broadcast_id,client_id,lg)
            lg.info(f"Response from func_broadcast_excel_download function  is {result}")
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "analytics_excel_download API failed: " + str(e))
            return jsonify({"id": "1221", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})    

    
    @app_analytics.route('/get_client_send_to_wa_analytics',methods = ['POST'])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_get_client_send_to_wa_analytics():
        """ Function for getting client send to wa analytics """
        data = request.json
        str_client_number = data["client_number"]
        int_from_timestamp = data["from"]
        int_to_timestamp = data["to"]        
        bool_all = data["all"]
        if bool_all == True:
            str_template_name = "None"
        else:
            str_template_name = data["template_name"]

        try:
            obj_analytics = ClsDmpServiceAnalytics(str_client_number)
            flag = "client"
            result = obj_analytics.func_get_send_to_wa_analytics(str_template_name,int_from_timestamp,int_to_timestamp,bool_all,flag,lg)
            return result
        except Exception as e:
            lg.critical("list_of_numbers API failed : " + str(e))
            return jsonify({"id": "2001", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})


    @app_analytics.route('/download_client_api_analytics',methods = ['POST'])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_download_client_api_analytics():
        data = request.json
        str_client_number = data["client_number"]
        str_template_name = data["template_name"]
        int_from_timestamp = data["from"]
        int_to_timestamp = data["to"]   

        try:
            obj_analytics = ClsDmpServiceAnalytics(str_client_number)
            result = obj_analytics.func_download_client_api_analytics(str_template_name,int_from_timestamp,int_to_timestamp,lg)
            return result
        except Exception as e:
            lg.critical("list_of_numbers API failed : " + str(e))
            return jsonify({"id": "2001", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})

    
    @app_analytics.route('/broadcast_billing_analytics',methods=["POST"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def billing_basic_analytics():
        """Fuction called to get analytics of billing in given time range"""
        claims = get_jwt()
        client_id = claims["ew_id"]
        email_id = claims["email_id"]
        try:
            all = request.json["all"]
            if all == False:
                client_number = request.json["client_number"]                
            else:
                client_number = claims["accessible_phones"][0]
            int_from_timestamp = request.json["start_time"]
            int_to_timestamp = request.json["end_time"]
            str_template_name = request.json["template_name"]
            all_template = request.json["all_template"]
            obj_broadcast_analytics = ClsBroadcastAnalysis(client_number)
            result = obj_broadcast_analytics.broadcast_basic_billing_analytics(client_number,client_id,all,int_from_timestamp,int_to_timestamp,email_id,str_template_name,all_template,lg)
            return result
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "broadcast_billing_analytics API failed: " + str(e))
            return jsonify({"id": "3011", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})

    @app_analytics.route('/get_client_send_to_wa_billing_analytics',methods = ['POST'])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_get_client_send_to_wa_billing_analytics():
        """ Function for getting client send to wa api billing analytics """
        data = request.json
        str_client_number = data["client_number"]
        int_from_timestamp = data["from"]
        int_to_timestamp = data["to"]        
        bool_all = data["all"]
        if bool_all == True:
            str_template_name = "None"
        else:
            str_template_name = data["template_name"]

        try:
            obj_analytics = ClsDmpServiceAnalytics(str_client_number)
            flag = "client"
            result = obj_analytics.func_get_send_to_wa_analytics(str_template_name,int_from_timestamp,int_to_timestamp,bool_all,flag,lg)
            return result
        except Exception as e:
            lg.critical(" failed : " + str(e))
            return jsonify({"id": "4001", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})
    
    @app_analytics.route('/conversational_billing_analytics',methods = ['POST'])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    @token_required
    def func_conversational_billing_analytics():
        """ Function for getting client send to conversational  billing analytics """
        data = request.json
        str_client_number = data["client_number"]
        int_from_timestamp = data["from"]
        int_to_timestamp = data["to"]        
        try:
            obj_analytics = ClsBroadcastAnalysis(str_client_number)
            result = obj_analytics.func_send_conversational_analytics(int_from_timestamp,int_to_timestamp,lg)
            return result
        except Exception as e:
            lg.critical("Conversational_billing API failed : " + str(e))
            return jsonify({"id": "8001", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})

    @app_analytics.route('/get_data_billing_analytics',methods = ['POST'])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])  
    @token_required
    def get_data_billing_analytics():
        claims = get_jwt()
        client_id = claims["ew_id"]
        email_id = claims["email_id"]
        try:
            all = request.json["all"]
            if all == False:
                client_number = request.json["client_number"]                
            else:
                client_number = claims["accessible_phones"][0]
            int_from_timestamp = request.json["start_time"]
            int_to_timestamp = request.json["end_time"]
            # page_size = request.json["page_size"]
            # page_num = request.json["page_num"]
            str_template_name = request.json["template_name"]
            all_template = request.json["all_template"]
            lg.info(f"requestisfine")
            obj_broadcast_analytics = ClsBroadcastAnalysis(client_number)
            result = obj_broadcast_analytics.broadcast_basic_billing_analytics(client_number,client_id,all,int_from_timestamp,int_to_timestamp,email_id,str_template_name,all_template,lg)
            result=result.json
            lg.info(f"resultis{result}")
            # result=result.text
            lg.info(f"typeofresultis{type(result)}")
            # result=json.loads(result)
            broadcast_cost = result.get("data", {}).get("broadcast_sum", {}).get("cost_sum"," ")
            lg.info(f"Broadcast_cost is {broadcast_cost}")
        except Exception as e:
                lg.critical("broadcast_billing_function failed : " + str(e))
                return jsonify({"id": "10001", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})
        
        try:
            obj_analytics = ClsDmpServiceAnalytics(client_number)
            flag = "client"
            result = obj_analytics.func_get_send_to_wa_analytics(str_template_name,int_from_timestamp,int_to_timestamp,all,flag,lg)
            # result=result.get_data(as_text=True)
            lg.info(f"typeofresultis{type(result)}")
            if type(result)!=dict:
                result=result.json
                if all == False:
                    clientapi_cost = result.get("total_cost"," ")
                    lg.info(f"clientapi_cost is {clientapi_cost}")
            elif all == True :
                clientapi_cost = result.get("total_count", {}).get("total_cost"," ")
                lg.info(f"clientapi_cost is {clientapi_cost}")
            else:
                if all == False:
                    clientapi_cost = result.get("total_cost"," ")
                    lg.info(f"clientapi_cost is {clientapi_cost}")
                elif all == True :
                    clientapi_cost = result.get("total_count", {}).get("total_cost"," ")
                    lg.info(f"clientapi_cost is {clientapi_cost}")
        
        except Exception as e:
                lg.critical("clientapi_billing_function failed : " + str(e))
                return jsonify({"id": "10002", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})
        try:
            obj_analytics = ClsBroadcastAnalysis(client_number)
            result = obj_analytics.func_send_conversational_analytics(int_from_timestamp,int_to_timestamp,lg)
            # result=result.json
            lg.info(f"typeofresultis{type(result)}")
            conversational_cost = result.get("total_count",{}).get("total_cost","")
            lg.info(f"conversational_cost is {conversational_cost}")
        except Exception as e:
                lg.critical("conversational_billing_function failed : " + str(e))
                return jsonify({"id": "10003", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})
        try:
            return jsonify({"id": "10004", "message": "Data fetched successfully", "description": "", "data":{"broadcast_cost": broadcast_cost,"clientapi_cost":clientapi_cost,"conversational_cost":conversational_cost}, "success": True})
        except Exception as e:
                lg.critical("get_data_billing_analytics API failed : " + str(e))
                return jsonify({"id": "10005", "message": "Invalid data", "description": "Something went wrong in payload or result", "data": "", "success": False})




    

    