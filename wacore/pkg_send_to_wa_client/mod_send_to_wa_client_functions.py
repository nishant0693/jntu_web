import os.path
import datetime
import requests
import json
from flask import jsonify
from urllib.parse import urlparse
from datetime import datetime, timedelta

# Import custom packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from ..pkg_analytics.mod_analytics_function import ClsDmpServiceAnalytics
from wacore.pkg_extras.mod_common import ClsCommon


class ClsClientWhatsappFunc():
    """ Class called for client api function """


    def __init__(self,db_client_waba_settings):
        """ Create or initialize object and variables """
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        self.db_client_waba_settings = db_client_waba_settings
        if db_client_waba_settings != None:
            self.client_db_name = db_client_waba_settings["response"]["ew_id"].lower() + "_" + db_client_waba_settings["response"]["waba_id"]
            self.cl_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)
        


    def func_get_ewid_phone(self,str_bot_id):
        try:
            db_client_business_info = self.ew_db.client_business_info.find_one({"bot_id": str_bot_id}, {"_id":0})
        except:
            return jsonify({"id": "2091", "msg": "Data Query Error", "description": "", "data": "", "success": False})
        if db_client_business_info != None:
            return jsonify({"ew_id": db_client_business_info["ew_id"], "phone_number": db_client_business_info["wa_phone_numbers"][0]["wa_number"]})
        else:
            return jsonify({"id": "2092", "msg": "User information not present", "description": "", "data": "", "success": False})
        
        
    def func_list_of_numbers(self,str_bot_id,lg):
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"bot_id": str_bot_id})
            
            if db_client_waba_settings == None:
                lg.critical("bot_id=" + str(str_bot_id) + " | " + "DB error- client_waba_settings : None")    
                return jsonify({"id": "2022", "msg": "Invalid user input", "description": "Invalid client ID", "data": "", "success": False})        
            db_client_business_info = self.ew_db.client_business_info.find_one({"ew_id": db_client_waba_settings["ew_id"]}, {"_id":0})

            if db_client_business_info == None:
                lg.critical("bot_id=" + str(str_bot_id) + " | " + "DB error- client_business_info : None")
                return jsonify({"id": "2023", "msg": "Invalid user input. Invalid client id", "description": "Invalid client ID", "data": "", "success": False})
        except Exception as e:
            lg.critical("bot_id=" + str(str_bot_id) + " | " + "DB error- client_waba_settings. client_business_info : " + str(e))
            return jsonify({"id": "2024", "msg": "Data Query Error", "description": "", "data": "", "success": False})
        
        lst_phone_number = []
        for numbers in db_client_business_info["wa_phone_numbers"]:
            lst_phone_number.append({"phone_number": numbers["country_code"][1:] + "-" + numbers["wa_number"], "display_name": numbers["wa_display_name"]})
        return jsonify({"id": "2025", "msg": "Phone numbers fetched successfully", "description": "", "phone_number_details": lst_phone_number, "success": True})


    def func_list_of_templates(self,str_bot_id,lg):
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"bot_id": str_bot_id})
            if db_client_waba_settings == None:
                lg.critical("bot_id=" + str(str_bot_id) + " | " + "DB error- client_waba_settings : None")
                return jsonify({"id": "2034", "msg": "Invalid data. Invalid bot id", "success": False})
            db_template_mapping = list(self.cl_db.template_mapping.find({"ew_id": db_client_waba_settings["ew_id"], "template_status": "APPROVED"}, {"_id":0, "template_id":1, "template_name":1}))        
        except Exception as e:
            lg.critical("bot_id=" + str(str_bot_id) + " | " + "DB error- client_waba_settings : " + str(e))
            return jsonify({"id": "2035", "msg": "Data Query Error", "description": "", "data": "", "success": False})
        return jsonify({"id": "2036", "msg": "List of templates fetched successfully", "description": "", "list_of_templates": db_template_mapping, "success": True})


    def func_dynamic_fields_details(self,bot_id,template_name,lg):
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"bot_id": bot_id})
            db_template_mapping = self.cl_db.template_mapping.find_one({"ew_id": db_client_waba_settings["ew_id"], "template_name":template_name}, {"id":0})
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - client_waba_settings, template_mapping: " + str(e))
            db_template_mapping = None
            return jsonify({"id": "2044", "msg": "Data Query Error", "description": "", "data": "", "success": False})

        if db_template_mapping == None:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - client_waba_settings : None")
            return jsonify({"id": "2045", "msg": "Invalid user input. Template doesn't exist", "description": "Template doesnt exist in database", "data": "", "success": False})

        try:
            dict_dynamic_field = {}
            int_count = 0
            var_body = db_template_mapping["body"]
            dict_dynamic_field.update({"body_column_names": var_body["body_column_names"]})
            int_count = int_count + len(var_body["body_column_names"])   

            if "header" in db_template_mapping:
                if db_template_mapping["header"]["header_type"] == "None":
                    str_header_data = None
                elif db_template_mapping["header"]["header_type"] == "TEXT":
                    dict_dynamic_field.update({"header_text_column_name": db_template_mapping["header"]["header_text_column_name"]})
                    int_count = int_count + 1
                else: 
                    dict_dynamic_field.update({"header_attachment_column_name": db_template_mapping["header"]["attachment_column_name"]})
                    int_count = int_count + 1
                        
            if "button" in db_template_mapping:
                if "button1" in db_template_mapping["button"]:
                    if "quick_reply" not in db_template_mapping["button"]["button1"]["type"]:
                        if "URL" in db_template_mapping["button"]["button1"]["call_to_action_type"]:
                            dict_dynamic_field.update({"button_column_name": db_template_mapping["button"]["button1"]["dynamic_url_column_name"]})
                            int_count = int_count + 1
                                            
                if "button2" in db_template_mapping["button"]:
                    if "URL" in db_template_mapping["button"]["button2"]["call_to_action_type"]:
                        dict_dynamic_field.update({"button_column_name": db_template_mapping["button"]["button2"]["dynamic_url_column_name"]})
                        int_count = int_count + 1
            dict_resp = {"dynamic_field_names": dict_dynamic_field, "dynamic_field_count": int_count}
            return jsonify({"id": "2046", "msg": "Data fetched successfully", "description": "", "response": dict_resp, "success": True})
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "Error in key response : " + str(e))
            return jsonify({"id": "2047", "msg": "Invalid key input", "description": "Something went wrong with key", "data": "", "success": False})    
    

    def func_create_json(self,bot_id,str_ew_id,str_phone_number,data,lg):
        try:
            db_template_mapping = self.cl_db.template_mapping.find_one({"ew_id": self.db_client_waba_settings["response"]["ew_id"], "template_name": data["template_name"]}, {"id":0})
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - template_mapping: " + str(e))
            db_template_mapping = None
            return jsonify({"id": "2065", "response": "Data Query Error", "description": "Data Query error", "data": "", "success": False})

        if db_template_mapping == None:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - template_mapping: None")
            return jsonify({"id": "2066", "response": "Invalid user input", "description": "Template doesnt exist in database", "data": "", "success": False})

        try:
            dict_components = {}
            var_body = db_template_mapping['body']
            lst_body = []
            for str_body_params in var_body['body_column_names']:
                lst_body.append({str_body_params: 'sample ' + str_body_params})
            body_data = {'body': lst_body}

            if 'header' in db_template_mapping:
                if db_template_mapping['header']['header_type'] == 'None':
                    var_header = None
                elif db_template_mapping['header']['header_type'] == 'TEXT':
                    if db_template_mapping['header']['header_text_column_name'] == "":
                        var_header = None
                    else:
                        var_header = {'header': {'type': 'text', str(db_template_mapping['header']['header_text_column_name']): 'sample ' + str(db_template_mapping['header']['header_text_column_name'])}}
                else:
                    var_header = {'header': {'type': db_template_mapping['header']['attachment_type'], 'link': 'sample_attachment_url'}}      
            else:
                var_header = None
            
            if 'button' in db_template_mapping:
                if 'button1' in db_template_mapping['button']:
                    if 'quick_reply' not in db_template_mapping['button']['button1']['type']:
                        if 'URL' in db_template_mapping['button']['button1']['call_to_action_type']:
                            if db_template_mapping['button']['button1']['dynamic_url_column_name'] != "":
                                var_button = {'button':{'url': db_template_mapping['button']['button1']['dynamic_url_column_name']}}
                            else:
                                var_button = None
                        else:
                            var_button = None
                    else:
                        var_button = None
                if 'button2' in db_template_mapping['button']:
                    if 'URL' in db_template_mapping['button']['button2']['call_to_action_type']:
                        if db_template_mapping['button']['button2']['dynamic_url_column_name'] != "":
                            var_button = {'button':{'url': db_template_mapping['button']['button2']['dynamic_url_column_name']}}
                        else:
                            var_button = None
            else:
                var_button = None
            
            if var_header != None:
                dict_components.update(var_header)
            
            if var_button != None:
                dict_components.update(var_button)
            
            dict_components.update(body_data)

            resp_json = {"api": "WA", "waid": "1d4aa0-1203-4c85-8dc3", "version": "v1", "type": "template", "template_name": db_template_mapping["template_name"], "payload": {"from": "sample_sender_contact", "to": "sample_recipient_contact", "components": dict_components}}
            return jsonify({"id": "2067", "msg": "Template data fetched successfully", "description": "", "response": resp_json, "success": True})
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "Error in key response : " + str(e))
            return jsonify({"id": "2068", "response": "Invalid key input", "description": "Something went wrong in key", "data": "", "success": False}) 
            
            
    def func_send_templates_with_details(self,bot_id,lg):
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"bot_id": bot_id})
            if db_client_waba_settings == None:
                lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - template_mapping: None")
                return jsonify({"id": "2054", "message": "Invalid user input", "description": "Invalid bot id", "data": "", "success": False})
            db_template_mapping = list(self.cl_db.template_mapping.find({"ew_id": db_client_waba_settings["ew_id"]}, {"_id":0})) 
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - client_waba_settings, template_mapping: " + str(e))
            return jsonify({"id": "2055", "msg": "Data Query Error", "description": "", "data": "", "success": False})

        if db_template_mapping == None:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - template_mapping : None")
            return jsonify({"id": "2056", "msg": "Invalid user input. Template doesnt exist", "description": "Template doesnt exist in database", "data": "", "success": False})

        try:
            dict_resp = {}
            for template in db_template_mapping:
                dict_dynamic_field = {}
                body = template['body']
                if body['body_column_names'] != []:
                    dict_dynamic_field.update({"body": body['body_column_names']})   

                if 'header' in template:
                    if template['header']['header_type'] == 'None':
                        str_header_data = None
                    elif template['header']['header_type'] == 'TEXT':
                        dict_dynamic_field.update({"header_text": [template['header']['header_text_column_name']]})
                    else: 
                        dict_dynamic_field.update({"header_attachment": [template['header']['attachment_column_name']]})
                
                if 'button' in template:
                    if 'button1' in template['button']:
                        if 'quick_reply' not in template['button']['button1']['type']:
                            if 'URL' in template['button']['button1']['call_to_action_type']:
                                dict_dynamic_field.update({"button": [template['button']['button1']['dynamic_url_column_name']]})
                                            
                    if 'button2' in template['button']:
                        if 'URL' in template['button']['button2']['call_to_action_type']:
                            dict_dynamic_field.update({"button": [template['button']['button2']['dynamic_url_column_name']]})

                dict_resp.update({template["template_name"]: dict_dynamic_field})
            return jsonify({"id": "2057", "msg": "Data fetched successfully", "description": "", "response": dict_resp, "success": False})
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "Error in key response : " + str(e))
            return jsonify({"id": "2058", "msg": "Invalid user key input", "description": "Something went wrong in key input", "description": "", "data": "", "success": False})


class ClsSendTemplate():
    def __init__(self,db_client_waba_settings,flag):
        """ Create or initialize object and variables """
        self.flag = flag
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        self.db_client_waba_settings = db_client_waba_settings
        self.client_db_name = db_client_waba_settings["response"]["ew_id"].lower() + "_" + db_client_waba_settings["response"]["waba_id"]
        self.cl_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)
        self.contact_collection_name = "contacts_" + db_client_waba_settings["response"]["client_number"]
        pass


    def func_send_single_template(self,message_data,analytics_arguments,bot_id,lg):
        """ Send single template message """
        try:
            db_template_mapping = self.cl_db.template_mapping.find_one({"ew_id": self.db_client_waba_settings["response"]["ew_id"], "template_name": message_data["template_name"]}, {"_id":0})
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - client_waba_settings, template_mapping : " + str(e))
            db_template_mapping = None
            return jsonify({"id": "2074", "response": "Data Query Error", "description": "", "data": "", "success": False})
        
        if db_template_mapping == None:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - client_waba_settings, template_mapping : None")
            return jsonify({"id": "2075", "response": "Invalid user input. Invalid template name", "description": "Invalid template name", "data": "", "success": False})
        
        try:
            sender_contact = message_data['payload']['from']
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "Error in recipient contact : " + str(e))
            sender_contact = None
            return jsonify({"id": "2076", "response": "Invalid user input", "description": "Please, enter appropriate sender contact", "data": "", "success": False})
        
        try:
            # str_contact_status = self.func_get_contact_status(message_data,bot_id,lg)
            str_contact_status="valid"
            if str_contact_status == "valid":
                dict_message_body = self.func_create_message_request_body(message_data,db_template_mapping,bot_id,lg)
                var_message_request = self.func_send_message_request(dict_message_body,analytics_arguments,bot_id,lg)

                if 'messages' in var_message_request.keys():
                    return jsonify({"id": "2077","message_id": var_message_request["messages"][0]["id"], "response": "Message request send successfully", "description": "", "data": "", "success": True})
                else:
                    return jsonify({"id": "2080", "response": "Message sending failed", "description": "Message sending failed", "data": "", "success": True})
                   
            # else:
            #     template_name = analytics_arguments["template_name"]
            #     str_client_number = analytics_arguments["client_number"]
            #     recipient_number = analytics_arguments["recipient_number"]
            #     client_id =self.db_client_waba_settings["response"]["ew_id"]
            #     obj_analytics = ClsDmpServiceAnalytics(str_client_number)
            #     obj_analytics.func_send_to_wa_invalid(str_contact_status,recipient_number,client_id,template_name,self.flag,bot_id,lg)
            #     return jsonify({"id": "2078", "response": "invalid user input", "description": "Invalid contact", "data": "", "success": True})
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "Error in key response : " + str(e))
            recipient_contact = None
            return jsonify({"id": "2079", "response": "Invalid Payload", "description": "", "data": "", "success": False})

    ## Removed after v2.45.1
    # def func_get_contact_status(self,message_data,bot_id,lg):
    #     """ Get contact status """
    #     try:
    #         lst_valid_recipient_from_db = self.cl_db[self.contact_collection_name].distinct("recipient_number")
    #     except Exception as e:
    #         lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - contacts : " + str(e))
    #         lst_valid_recipient_from_db = None
    #         return jsonify({"id": "2080", "response": "Data Query Error", "description": "", "data": "", "success": False})
    #     if message_data["payload"]["to"] in lst_valid_recipient_from_db:
    #         status = "valid"
    #     else:
    #         url = self.db_client_waba_settings["response"]["url"] + "contacts"
    #         token = ClsCommon().func_get_access_token(self.db_client_waba_settings["response"])
    #         payload= {"blocking": "wait", "contacts": ["+" + message_data["payload"]["to"]]} # 'force_check': True   ...is removed
    #         headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
    #         response = requests.request('POST', url, headers=headers, data=json.dumps(payload))
    #         json_resp = response.json()
    #         if json_resp["contacts"][0]["status"] == "valid":
    #             self.cl_db[self.contact_collection_name].insert_one({"recipient_number": message_data["payload"]["to"], "expiry": int((datetime.datetime.now() + datetime.timedelta(days=7)).timestamp())})
    #             status = "valid"
    #         else:
    #             status = "invalid"
    #     return status


    def func_create_message_request_body(self,message_data,db_template_mapping,bot_id,lg):
        """ Create message request body """
        try:
            var_body = message_data["payload"]["components"]["body"]
            lg.info(f"message_data is {message_data}")
            lg.info(f"db_template_mappingdatais{db_template_mapping}")
            lst_text = []
            for dict_body in var_body:
                var_text_data = {"type": "text", "text": list(dict_body.values())[0]}
                lst_text.append(var_text_data)
            dict_component_data = [{"type": "body", "parameters": lst_text}]

            if 'header' in message_data["payload"]["components"]:
                if message_data["payload"]["components"]["header"]["type"] == "text":
                    dict_header = message_data["payload"]["components"]["header"]
                    header_text_keys = list(dict_header.keys())
                    header_text_values = list(dict_header.values())
                    if header_text_keys[0] == "type":
                        header_text = header_text_values[1]
                    if header_text_keys[1] == "type":
                        header_text = header_text_values[0]
                    
                    dict_header_data = {"type": "header", "parameters": [{"type": "text", "text": header_text}]}
                elif message_data["payload"]["components"]["header"]["type"] == "LOCATION":
                    dict_header = message_data["payload"]["components"]["header"]
                    header_text_keys = list(dict_header.keys())
                    header_text_values = list(dict_header.values())
                    if header_text_keys[0] == "type":
                        location_text = header_text_values[1].split(" ")
                        lg.info("location values after split is {location_text}")
                        header_text = {"latitude":location_text[0], "longitude":location_text[1],"name":location_text[2],"address":location_text[3]}
                    dict_header_data = {"type": "header", "parameters": [{"type": "location", "location": header_text}]}
                else:
                    if str(message_data['payload']['components']['header']['type']) == 'document':
                        filename = self.func_add_filename(message_data['payload']['components']['header']['link'])
                        dict_header_data = {"type": "header", "parameters": [{"type": message_data['payload']['components']['header']['type'], str(message_data['payload']['components']['header']['type']): {'link': message_data['payload']['components']['header']['link'], 'filename': filename}}]}             
                    else:
                        dict_header_data = {"type": "header", "parameters": [{"type": message_data['payload']['components']['header']['type'], str(message_data['payload']['components']['header']['type']): {'link': message_data['payload']['components']['header']['link']}}]}
            else: 
                dict_header_data = None
            
            if 'button' in message_data['payload']['components']:
                if 'button' in db_template_mapping:
                    if 'button1' in db_template_mapping['button'] and db_template_mapping['button']['button1']['call_to_action_type'] == 'URL':
                        index = 0
                    elif 'button2' in db_template_mapping['button'] and db_template_mapping['button']['button2']['call_to_action_type'] == 'URL':
                        index = 1
                else:
                    pass
                dict_button_data = {"type": "button", "sub_type" : "url", "index": index, "parameters": [{"type": "text", "text": message_data['payload']['components']['button']['url']}]}   
            else: 
                dict_button_data = None
            
            try:
                if 'button1' in db_template_mapping['button'] and db_template_mapping['button']['button1']['type'] == 'quick_reply':
                    dict_button_data = []
                    count = 0
                    for qr in db_template_mapping['button']['button1']['quick_reply_list']:
                        count = count + 1
                        dict_qr_data = {"type": "button", "sub_type": "quick_reply", "index": count-1, "parameters": [{"type": "payload", "payload": qr["payload"]}]}
                        dict_button_data.append(dict_qr_data)
            except Exception as e:
                dict_button_data = None

            if dict_header_data != None:
                dict_component_data.append(dict_header_data)
            
            if dict_button_data != None:
                if isinstance(dict_button_data, list) == False:
                    dict_component_data.append(dict_button_data)
                else:
                    dict_component_data = dict_component_data + dict_button_data
            # data = json.dumps({"to": message_data["payload"]["to"], "type": "template", "template": {"namespace": self.db_client_waba_settings["response"]["namespace"], "name": message_data["template_name"], "language": {"policy": "deterministic", "code": "en"}, "components": dict_component_data}})
            data = json.dumps({"messaging_product": "whatsapp", "recipient_type": "individual","to": message_data["payload"]["to"], "type": "template", "template": {"name": message_data["template_name"], "language": {"policy": "deterministic", "code": db_template_mapping.get("language","en")}, "components": dict_component_data}})
            return data
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - client_waba_settings, template_mapping: " + str(e))
            return jsonify({"id": "2081", "response": "Invalid key input", "description": "Something went wrong in key", "data": "", "success": False})


    def func_send_message_request(self,message_body,analytics_arguments,bot_id,lg):
        """ Send message request """
        # url_templates = self.db_client_waba_settings["response"]["url"] + "messages/"
        url_templates = self.db_client_waba_settings["response"]["url"] + self.db_client_waba_settings['response']['Phone-Number-ID']+"/messages"
        wa_token = self.db_client_waba_settings['response']['access_token']
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer '+ str(wa_token)}
        response_templates = requests.request("POST", url_templates, headers=headers, data=message_body, verify=False)
        resp = response_templates.json()
        #-------------Added for Analytics-------------------------
        str_recipient_number = analytics_arguments["recipient_number"]
        str_template_name = analytics_arguments["template_name"]
        str_client_number =  analytics_arguments["client_number"]
        obj_analytics = ClsDmpServiceAnalytics(str_client_number)
        
        if 'messages' in resp.keys():
            str_message_id =  resp["messages"][0]["id"]
            obj_analytics.func_send_to_wa_analytics(str_recipient_number,str_message_id,str_template_name,self.flag,bot_id,lg,message_body,resp)
        else:
            lg.info("invalid number : " + str(str_recipient_number))
            str_message_id = "0"
            str_client_id = "0"
            obj_analytics.func_send_to_wa_invalid("invalid",str_recipient_number,str_client_id,str_template_name,self.flag,bot_id,lg,resp)

        #------------------------Analytics End----------------------
        return resp


    def func_add_filename(self,media_link):
        obj_filename = urlparse(media_link)
        filename = str(os.path.basename(obj_filename.path))
        if len(filename) > 60:
            filename = filename[:59]
        return filename

    def get_dynamic_variables_for_journey(self,recipient_number,client_number,lg):
        now_timestamp = datetime.now()
        ago = now_timestamp - timedelta(hours=24)
        to_timestamp = int(ago.timestamp())
        from_timestamp = int(now_timestamp.timestamp()) 

        client_api_list_documents = list(self.cl_db.client_sent_to_wa_analytics.aggregate([{"$match": {"client_number": client_number}},           
                                        {"$addFields":{"matching_doc": {"$filter": {"input" : "$success_stats", "as":"part","cond":{"$and":[{"$gt":["$$part.timestamp",to_timestamp]}, {"$lt":["$$part.timestamp",from_timestamp]},{"$eq":["$$part.recipient_number",recipient_number]}]}}} }}]))                              
        brodcast_list_documents = list(self.cl_db.broadcast_details.aggregate([{"$match": {"client_number": client_number,"timestamp":{"$gt":to_timestamp,"$lt": from_timestamp}}},           
                                        {"$addFields":{"matching_doc": {"$filter": {"input" : "$success_stats", "as":"part","cond":
                                        { "$eq":["$$part.recipient_number",recipient_number]}} }}}]))
        dmps_send_to_wa_list_documents = list(self.cl_db.dmps_send_to_wa_analytics.aggregate([{"$match": {"client_number": client_number}},          
                                    {"$addFields":{"matching_doc": {"$filter": {"input" : "$success_stats", "as":"part","cond":{"$and":[{"$gt":["$$part.timestamp",to_timestamp]}, {"$lt":["$$part.timestamp",from_timestamp]},{"$eq":["$$part.recipient_number",recipient_number]}]}}} }}])) 
        
        client_api_dynamic_variables = []
        for i in client_api_list_documents:
            obj = {}
            dynamics = []
            template_name = i["template_name"]
            obj["template_name"] = template_name
            column_names = self.cl_db.template_mapping.find_one({"template_name":template_name},{"_id":0,"header":1,"body":1,"footer":1})
            dynamic_variable_column_names = {"header_attachment_column_name":column_names["header"]["attachment_column_name"],"header_text_column_name":column_names["header"]["header_text_column_name"],"body_column_names":column_names["body"]["body_column_names"]}
            for j in i["matching_doc"]:    
                dynamic_var =  j["dynamic_variables"]  
                message_id_button_resp = {}  
                if j.get("button_response"):
                    message_id_button_resp["button_response"] = j["button_response"]                
                message_id_button_resp["message_id"] = j["message_id"]
                message_id_button_resp.update(dynamic_variable_column_names)
                dynamic_var.append(message_id_button_resp)
                dynamics.append(dynamic_var)
            obj["dynamic_variables"] = dynamics
            if obj["dynamic_variables"] == []:
                    obj.pop("template_name")
            else:
                client_api_dynamic_variables.append(obj)

        dmps_send_to_wa_dynamic_variables = []
        for i in dmps_send_to_wa_list_documents:
            obj = {}
            dynamics = []
            template_name = i["template_name"]
            obj["template_name"] = template_name
            column_names = self.cl_db.template_mapping.find_one({"template_name":template_name},{"_id":0,"header":1,"body":1,"footer":1})
            dynamic_variable_column_names = {"header_attachment_column_name":column_names["header"]["attachment_column_name"],"header_text_column_name":column_names["header"]["header_text_column_name"],"body_column_names":column_names["body"]["body_column_names"]}

            for j in i["matching_doc"]:             
                dynamic_var =  j["dynamic_variables"]  
                message_id_button_resp = {}  
                if j.get("button_response"):
                    message_id_button_resp["button_response"] = j["button_response"]                
                message_id_button_resp["message_id"] = j["message_id"]
                message_id_button_resp.update(dynamic_variable_column_names)
                dynamic_var.append(message_id_button_resp)
                dynamics.append(dynamic_var)
            obj["dynamic_variables"] = dynamics
            if obj["dynamic_variables"] == []:
                    obj.pop("template_name")
            else:
                dmps_send_to_wa_dynamic_variables.append(obj)
    
        broadcast_dynamic_variables = []
        for i in brodcast_list_documents:
            obj = {}
            dynamics = []
            template_name = i["template_name"]
            obj["template_name"] = template_name
            column_names = self.cl_db.template_mapping.find_one({"template_name":template_name},{"_id":0,"header":1,"body":1,"footer":1})
            dynamic_variable_column_names = {"header_attachment_column_name":column_names["header"]["attachment_column_name"],"header_text_column_name":column_names["header"]["header_text_column_name"],"body_column_names":column_names["body"]["body_column_names"]}

            for j in i["matching_doc"]:
                dynamic_var =  j["dynamic_variables"]  
                message_id_button_resp = {}  
                if j.get("button_response"):
                    message_id_button_resp["button_response"] = j["button_response"]                
                message_id_button_resp["message_id"] = j["message_response_id"]
                message_id_button_resp.update(dynamic_variable_column_names)
                dynamic_var.append(message_id_button_resp)
                dynamics.append(dynamic_var)
            obj["dynamic_variables"] = dynamics
            if obj["dynamic_variables"] == []:
                    pass
            else:
                broadcast_dynamic_variables.append(obj)

        return jsonify({"broadcast_dynamic_var":broadcast_dynamic_variables, "client_api_dynamic_variables":client_api_dynamic_variables,"dmps_send_to_wa_dynamic_variables":dmps_send_to_wa_dynamic_variables,"success": True})