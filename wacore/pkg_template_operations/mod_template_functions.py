import requests
import json
import time
import datetime
import ast
from flask import jsonify
from wacore.auth.mod_login_functions import phone_access_required

# Import custom packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from ..global_variable import graph_url, url_for_dmp


class ClsTemplateOperations():
    """ Class called to perform temprate actions """

    def __init__(self,db_client_waba_settings):
        """
        Initialize required object and variables:

        Parameters:
            client_number (str): Mobile Number of WhatsApp
            db_client_waba_settings (dict): All WABA Settings
        Local Variable:
            client_db_name(str): Fetch and generate DB name for individual client
            ew_db(DB object): Initialize DB
            client_db(DB Object):  Initialize DB
            contact_collection_name (collection Object name): Create collection name
        Returns:
            All above parameteres and variables
      
        """

        self.db_client_waba_settings = db_client_waba_settings
        try:
            self.client_db_name = db_client_waba_settings["response"]["ew_id"].lower() + "_" + db_client_waba_settings["response"]["waba_id"]
        except:
            self.client_db_name = ""
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        try:
            self.cl_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)
        except:
            pass    


    def func_get_all_templates(self,str_client_id,flag,lg):
        """ Method called to get list of template names """
        try:
            db_wa_system_account = self.ew_db.wa_system_account.find_one({})
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"ew_id": str_client_id}, {"_id":0})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - wa_system_account, client_waba_settings : " + str(e))    
            return jsonify({"id": "1042", "message": "Data Query Error", "description": "", "data": "", "success": False})
        if db_client_waba_settings == None:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings : None")    
            return jsonify({"id": "1043", "message": "Invalid user input", "description": "Invalid client id or client number", "data": "", "success": False})
        
        var_url = graph_url + str(db_client_waba_settings["waba_id"]) + "/message_templates?access_token=" + str(db_wa_system_account["system_user_token"])
        var_payload = {}
        var_headers = {}
        var_response = requests.request("GET", var_url, headers=var_headers, data=var_payload)
        var_response_json = var_response.json()
            
        lst_template = var_response_json['data']
        if flag == 'name':
            lst_result = []
            for template in lst_template:
                lst_result.append(template['name'])
        elif flag == 'name_status':
            lst_result = []
            for template in lst_template:
                data = {'template_name': template['name'], 'status': template['status'], 'language': template['language']}
                lst_result.append(data)
        elif flag == 'all':
            lst_result = json.loads(var_response.text)
        return jsonify({"id": "1044", "message": "Templates fetched successfully", "description": "", "data": lst_result, "success": True})

    
    def func_create_header_handle(self,obj_file,str_file_type,client_id,lg, media_id):
        """ Method called to create header handle """
        if str_file_type == "image":
            if obj_file.filename.lower().endswith((".png")):
                obj_file_data = obj_file
                type = "image/png"
            elif obj_file.filename.lower().endswith((".jpg")):
                obj_file_data = obj_file
                type = "image/jpg"
            else:
                lg.critical("ew_id=" + str(client_id) + " | " + "Invalid image format. File type must be .png or .jpg for image file")    
                return jsonify({"id": "1052", "message": "Invalid file format", "description": "File type must be .png or .jpg for image file", "data": "", "success": False})
            
        elif str_file_type == "video":
            if obj_file.filename.lower().endswith((".mp4")):
                obj_file_data = obj_file
                type = "video/mp4"
            else:
                lg.critical("ew_id=" + str(client_id) + " | " + "Invalid video format. File type must be .mp4 for video file")    
                return jsonify({"id":"1053", "message": "Invalid file format", "description": "File type must be .mp4 for video file", "data": "", "success": False})
        
        elif str_file_type == "document":
            doc_endswith = (".doc", ".docx", ".odt", ".pdf", ".ppt", ".pptx", ".xls", ".xlsx")
            if obj_file.filename.lower().endswith(doc_endswith):
                obj_file_data = obj_file
                type = "application/pdf"
            else:
                lg.critical("ew_id=" + str(client_id) + " | " + "Invalid document format. File type must be .pdf for document file")    
                return jsonify({"id": "1054", "message": "Invalid file format", "description": "File type must be .pdf for document file", "data": "", "success": False})
        else:
            lg.critical("ew_id=" + str(client_id) + " | " + "Invalid file format.")    
            return jsonify({"id": "1055", "message": "Invalid file format", "description": "File type must be image, video or document - 1", "data": "", "success": False})
        try:
            str_system_user_token = self.ew_db.wa_system_account.find_one({},{"_id":0})["system_user_token"]
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - wa_system_account : " + str(e))    
            return jsonify({"id": "1056", "message": "Data Query Error", "description": "", "data": "", "success": False})

        var_url1 = graph_url + "app/uploads?access_token=" +  str_system_user_token + "&file_length=100&file_type=" + type
        var_response1 = requests.request("POST", var_url1, headers={}, data={})
        if "id" not in var_response1.json():
            lg.critical("ew_id=" + str(client_id) + " | " + "Error in 1st WA API of create_header_handle : " + str(var_response1.json()) )    
            return jsonify({"id": "1057", "message": "Invalid file format", "description": "File type must be image, video or document - 2", "data": "", "success": False})

        var_url2 = graph_url + var_response1.json()["id"] + "&access_token=" + str_system_user_token
        var_payload2 = {'file_offset': '0', 'Authorization': 'OAuth ' + str_system_user_token, 'Host': 'graph.facebook.com', 'Connection': 'close', 'file_length': '6963', 'file_type': str_file_type, 'file': obj_file_data}
        var_response2 = requests.request("POST", var_url2, headers={}, data=var_payload2)
        if "h" not in var_response2.json():
            lg.critical("ew_id=" + str(client_id) + " | " + "Error in 2nd WA API of create_header_handle : " + str(var_response2.json()) )    
            return jsonify({"id": "1058", "message": "Invalid file format", "description": "File type must be image, video or document - 3", "data": "", "success": False})
        else:
            header_handle = var_response2.json()['h']

        
        
        db_data =  {"header_handler": header_handle  , "media_id": media_id}
        
        self.cl_db.media_headr_handler.insert_one(db_data)
        # self.cl_db.media_headr_handler.insert_one({"header_handler": header_handle  , "media_id": media_id})
    
        return jsonify({"id": "1059", "message": "Data fetched successfully", "description": "", "data": header_handle, "success": True})


    def func_create_message_template(self,str_client_number,str_client_id,str_email_id,bot_id,data,lg,flow_name,base_url,waba_id,access_token):
        """ Method called to create template and add relative fields in database """
        lst_body_column_names = []
        lst_body_example_field = []
        for new_data in data["body"]["body_data"]:
            str_body_column_name = '_'.join(str(new_data["body_column_names"]).strip().split()).upper()
            lst_body_column_names.append(str_body_column_name)
            lst_body_example_field.append(new_data["body_example_field"])

        dict_new_body = {"body_text": data["body"]["body_text"], "body_column_names": lst_body_column_names, "body_example_field": lst_body_example_field}
        data["body"] = dict_new_body
        var_resp = self.func_create_message_template_body(bot_id,str_client_id,data,lg,flow_name,base_url,waba_id,access_token)
        try:    
            db_wa_system_account = self.ew_db.wa_system_account.find_one({}, {'_id':0})   
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"ew_id":str_client_id})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings, wa_System_account : " + str(e))    
            return jsonify({"id": "1064", "message": "Data Query Error", "description": "", "data": "", "success": False})

        if db_client_waba_settings == None:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings : None")    
            return jsonify({"id": "1065", "message": "Invalid user input", "description": "Invalid client ID", "data": "", "success": False})
        
        var_url = graph_url + str(db_client_waba_settings['waba_id']) + "/message_templates?access_token=" + str(db_wa_system_account['system_user_token'])     
        var_resp.update({"access_token": str(db_wa_system_account["system_user_token"])})
        lg.info(f"payload for creation of template api is {var_resp}")
        var_response = requests.post(var_url, data=json.dumps(var_resp), headers={"Content-Type":"application/json"})
        if 'error' in var_response.json():
            lg.critical("ew_id=" + str(str_client_id) + " | " + "Error in WA template create api : " + str(var_response.json()))    
            try:
                if var_response.json()["error"]["message"] == "(#192) Param components[1]['buttons'][1]['phone_number'] is not a valid phone number." or var_response.json()["error"]["message"] == "(#192) Param components[1]['buttons'][0]['phone_number'] is not a valid phone number.":
                    var_resp_data = {"id": "1066", "message": "Please, enter valid phone number in button", "description": "Error in POST request", "data": "", "success": False}
                elif var_response.json()["error"]["error_user_title"] == "Content in this language already exists" or var_response.json()["error"]["error_user_title"] == "Template Category Doesn't Match":
                    var_resp_data = {"id": "1067", "message": "This template name already exist. Please, enter another template name", "description": "Failed to create template", "data": "", "success": False}
                else:
                    var_resp_data = {"id": "1068", "message": "Failed to create template", "description": "Error in POST request", "data": {'error_code': var_response.json()['error']['code'], 'error_message': var_response.json()['error']['error_user_msg'], 'error_title': var_response.json()['error']['error_user_title'], 'message': var_response.json()['error']['message']}, 'success': False}       
            except Exception as e:
                lg.critical("ew_id=" + str(str_client_id) + " | " + "Error in key response : " + str(e))    
                var_resp_data = {"id": "1069", "message": "Failed to create template", "description": "Error in POST request", "data": "", 'success': False}
            return jsonify(var_resp_data)
        else:
            db_data = {'wa_response_template_id': var_response.json()['id'], 'template_id': int(time.time()), 'ew_id': str_client_id, 'template_name': data['template_name'], 'category': var_response.json()['category'], 'language': data['language'], 'timestamp': int(time.time()), 'template_status': var_response.json()['status'], 'reason': '', 'header': {'header_type': data['header']['header_type'], 'attachment_type': data['header']['attachment_type'], 'attachment_url': data['header']['attachment_url'], 'attachment_column_name': '_'.join(str(data['header']['attachment_column_name']).strip().split()).upper(), 'header_text': data['header']['header_text'], 'header_text_column_name': '_'.join(str(data['header']['header_text_column_name']).strip().split()).upper(), 'header_text_example_field': data['header']['header_text_example_field']}, 'body': {'body_text': data['body']['body_text'], 'body_column_names': data['body']['body_column_names'], 'body_example_fields': data['body']['body_example_field']}, 'footer': {'footer_text': data['footer']['footer_text']}}
            lg.info(f"Template_data is {data}")
            if db_data:
                header_handle = data['header']['attachment_header_handle']
                lg.info(f"header handle is {header_handle}")
                template_name_a = data['template_name']
                lg.info(f"template_name updating  is {template_name_a}")
                self.cl_db.media_headr_handler.update_one({"header_handler": header_handle},{"$set": {"template_name": template_name_a}} )
                if header_handle != '':
                    lg.info(f"inside media present and need to update template_name")
                else:
                    lg.info(f"inside media not present and no need to update template_name")
                    pass


            if data['button']['type'] != "None":
                if data['button']["type"] == "callToAction":
                    if len(data['button']['call_to_action']) == 0:
                        dict_button_db_data = {'button': {}}
                    elif len(data['button']['call_to_action']) == 1:
                        dict_button_db_data = {'button': {'button1': data['button']['call_to_action'][0]}}
                    elif len(data['button']['call_to_action']) == 2:
                        dict_button_db_data = {'button': {'button1': data['button']['call_to_action'][0], 'button2': data['button']['call_to_action'][1]}}
                elif data['button']['type'] == "flow":
                    if len(data['button']['flow_data']) == 1:
                        url = f"{base_url}/{waba_id}/flows"
                        lg.info(f"base url is {base_url} and waba_id is {waba_id} and flow_api is {url}")
                        headers = {"Authorization": f"Bearer {access_token}"}
                        response = requests.get(url, headers=headers).json()
                        lg.info(f"response of flow_API is {response}")
                        flow_data = next((flow for flow in response['data'] if flow['name'] == flow_name), None)
                        lg.info(f"flow_data for selected flow is {flow_data}")
                        flow_id = flow_data['id']
                        text = data['button']['flow_data'][0].get('text')
                        lg.info(f"flow_data_text is {text}")
                        lg.info(f"flow_id is {flow_id}")
                        flow_data = data['button']['flow_data'][0]
                        flow_data['flow_id'] = flow_id
                        dict_button_db_data = {'button': {'button1': data['button']['flow_data'][0]}}
                else:
                    if 'quick_reply_list' in data['button'].keys():
                        lst_quick_reply = []
                        for qr in data['button']['quick_reply_list']:
                            if qr["payload_type"] == "journey":
                                dict_quick_rply = {"type": "quick_reply", "text": qr["text"], "payload": str({"J":qr["journey_name"],"T":"QJ","I":db_client_waba_settings["int_id"],"S":["MD"]}), "payload_type": qr["payload_type"]}
                            else:
                                dict_quick_rply = {"type": "quick_reply", "text": qr["text"], "payload": str({"J":"","T":"DEF","I":db_client_waba_settings["int_id"],"S":[]}), "payload_type": qr["payload_type"]}
                            lst_quick_reply.append(dict_quick_rply)
                        dict_button_db_data = {'button':{"button1": {"type": "quick_reply", "quick_reply_list": lst_quick_reply}}}         
                db_data.update(dict_button_db_data)
                if "button" in db_data.keys():
                    if "button1" in db_data["button"].keys() and "dynamic_url_column_name" in db_data["button"]["button1"].keys():
                        db_data["button"]["button1"]["dynamic_url_column_name"] = '_'.join(str(db_data["button"]["button1"]["dynamic_url_column_name"]).strip().split()).upper()
                    elif "button2" in db_data["button"].keys() and "dynamic_url_column_name" in db_data["button"]["button2"].keys():
                        db_data["button"]["button2"]["dynamic_url_column_name"] = '_'.join(str(db_data["button"]["button2"]["dynamic_url_column_name"]).strip().split()).upper()
            db_data.update({"template_access": data["template_access"], "created_by": str_email_id, "created_with_number": str_client_number})
            try:
                if  data['header']['media_option'] == False:
                    media_option = "off"
                else:
                    media_option = "on"
                db_data['header']['media_option'] = data['header']['media_option']
                db_data['media_option']= media_option

                self.cl_db.template_mapping.insert_one(db_data)
            except Exception as e:
                lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : " + str(e))    
                return jsonify({"id": "1070", "message": "Data Query Error", "description": "", "data": "", "success": False})

            return jsonify({"id": "1071", "message": "Template created successfully", "description": "", "data": "", "success": True})


    def func_edit_message_template(self,str_client_number,str_client_id,str_email_id,bot_id,data,lg,flow_name,base_url,waba_id,access_token):
        db_template_mapping = self.cl_db.template_mapping.find_one({"template_name": data["template_name"]}, {"_id":0})
        if db_template_mapping == None:
            return jsonify({"id": "1081", "message": "Template does not exists", "description": "", "data": "", "success": True})
        lst_body_column_names = []
        lst_body_example_field = []
        for new_data in data["body"]["body_data"]:
            str_body_column_name = '_'.join(str(new_data["body_column_names"]).strip().split()).upper()
            lst_body_column_names.append(str_body_column_name)
            lst_body_example_field.append(new_data["body_example_field"])

        dict_new_body = {"body_text": data["body"]["body_text"], "body_column_names": lst_body_column_names, "body_example_field": lst_body_example_field}
        data["body"] = dict_new_body
        var_resp = self.func_create_message_template_body(bot_id,str_client_id,data,lg,flow_name,base_url,waba_id,access_token)

        lg.info(f"var_resp value is {var_resp}")
        try:    
            db_wa_system_account = self.ew_db.wa_system_account.find_one({}, {"_id":0})   
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"ew_id":str_client_id})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings, wa_System_account : " + str(e))    
            return jsonify({"id": "1064", "message": "Data Query Error", "description": "", "data": "", "success": False})

        if db_client_waba_settings == None:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings : None")    
            return jsonify({"id": "1065", "message": "Invalid user input", "description": "Invalid client ID", "data": "", "success": False})
        
        var_url = graph_url + str(db_template_mapping["wa_response_template_id"]) + "?access_token=" + str(db_wa_system_account["system_user_token"])     
        var_resp.update({"access_token": str(db_wa_system_account["system_user_token"])})
        var_response = requests.post(var_url, data=json.dumps(var_resp), headers={"Content-Type":"application/json"})
        if 'error' in var_response.json():
            lg.critical("ew_id=" + str(str_client_id) + " | " + "Error in WA template create api : " + str(var_response.json()))    
            if "message" in var_response.json()["error"]:
                var_resp_data = {"id": "1069", "message": var_response.json()["error"]["error_user_msg"], "description": var_response.json()["error"]["error_user_title"], "data": "", "success": False}
            else:
                var_resp_data = {"id": "1069", "message": var_response.json()["error"], "description": "Failed to edit template", "data": "", "success": False}
            return jsonify(var_resp_data)
        else:
            db_data = {'category': data['category'], 'language': data['language'], 'timestamp': int(time.time()), 'template_status': 'PENDING', 'reason': '', 
            'header': {'header_type': data['header']['header_type'], 'attachment_type': data['header']['attachment_type'], 'attachment_url': data['header']['attachment_url'], 'attachment_column_name': '_'.join(str(data['header']['attachment_column_name']).strip().split()).upper(), 'header_text': data['header']['header_text'], 'header_text_column_name': '_'.join(str(data['header']['header_text_column_name']).strip().split()).upper(), 'header_text_example_field': data['header']['header_text_example_field']},
            'body': {'body_text': data['body']['body_text'], 'body_column_names': data['body']['body_column_names'], 'body_example_fields': data['body']['body_example_field']},
            'footer': {'footer_text': data['footer']['footer_text']}}
            if data['button']['type'] != "None":
                if data['button']["type"] == "callToAction":
                    if len(data['button']['call_to_action']) == 0:
                        dict_button_db_data = {'button': {}}
                    elif len(data['button']['call_to_action']) == 1:
                        dict_button_db_data = {'button': {'button1': data['button']['call_to_action'][0]}}
                    elif len(data['button']['call_to_action']) == 2:
                        dict_button_db_data = {'button': {'button1': data['button']['call_to_action'][0], 'button2': data['button']['call_to_action'][1]}}
                else:
                    if 'quick_reply_list' in data['button'].keys():
                        lst_quick_reply = []
                        for qr in data['button']['quick_reply_list']:
                            if qr['payload_type'] == 'journey':
                                dict_quick_rply = {"type": "quick_reply", "text": qr["text"], "payload": str({"J":qr["journey_name"],"T":"QJ","I":db_client_waba_settings["int_id"],"S":["MD"]}), "payload_type": qr["payload_type"]}
                            else:
                                dict_quick_rply = {"type": "quick_reply", "text": qr["text"], "payload": str({"J":"","T":"DEF","I":db_client_waba_settings["int_id"],"S":[]}), "payload_type": qr["payload_type"]}
                            lst_quick_reply.append(dict_quick_rply)
                        dict_button_db_data = {'button':{"button1": {"type": "quick_reply", "quick_reply_list": lst_quick_reply}}}         
                db_data.update(dict_button_db_data)
                if "button" in db_data.keys():
                    if "button1" in db_data["button"].keys() and "dynamic_url_column_name" in db_data["button"]["button1"].keys():
                        db_data["button"]["button1"]["dynamic_url_column_name"] = '_'.join(str(db_data["button"]["button1"]["dynamic_url_column_name"]).strip().split()).upper()
                    elif "button2" in db_data["button"].keys() and "dynamic_url_column_name" in db_data["button"]["button2"].keys():
                        db_data["button"]["button2"]["dynamic_url_column_name"] = '_'.join(str(db_data["button"]["button2"]["dynamic_url_column_name"]).strip().split()).upper()
            db_data.update({"template_access": data["template_access"], "created_by": str_email_id, "created_with_number": str_client_number})
            try:    
                self.cl_db.template_mapping.update_one({"wa_response_template_id": db_template_mapping["wa_response_template_id"], "template_id": db_template_mapping["template_id"], "ew_id": str_client_id, "template_name": db_template_mapping['template_name']}, {"$set": db_data})
            except Exception as e:
                lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : " + str(e))    
                return jsonify({"id": "1070", "message": "Data Query Error", "description": "", "data": "", "success": False})

            return jsonify({"id": "1071", "message": "Template edited successfully", "description": "", "data": "", "success": True})


    def func_message_template_operation(self,str_client_number, str_client_id, str_email_id, bot_id, data, lg, flow_name,base_url,waba_id,access_token):
        if data["template_api_type"] == "EDIT":
            return self.func_edit_message_template(str_client_number,str_client_id,str_email_id,bot_id,data,lg,flow_name,base_url,waba_id,access_token)
        else:
            return self.func_create_message_template(str_client_number,str_client_id,str_email_id,bot_id,data,lg,flow_name,base_url,waba_id,access_token)

 
    def func_create_message_template_body(self,bot_id,str_client_id,data,lg,flow_name,base_url,waba_id,access_token):
        """ Method called to create template body """
        dict_body_data = {'category': data['category'], 'name': data['template_name'], 'language': data['language']}
        #uncomment for new#dict_body_data = {'allow_category_change':data['allow_category_change'],'category': data['category'], 'name': data['template_name'], 'language': data['language']}
        
        lst_components_data = []
        lg.info(f"value of data is {data}")

        #body
        if data['body']['body_example_field'] == []:
            lg.info("Static body with no variable")
            lst_components_data.append({'type': 'BODY', 'text': data['body']['body_text']})
        else:
            lg.info("Dynamic body with variable")
            lst_components_data.append({'type': 'BODY', 'text': data['body']['body_text'], 'example': {'body_text': [data['body']['body_example_field']]}})

        #header
        if data['header']['header_type'] == 'TEXT':
            if data['header']['header_text_example_field'] != "":
                lg.info("Dynamic text header with variable")
                lst_components_data.append({'type': 'HEADER', "format": 'TEXT', 'text': data['header']['header_text'], 'example': {'header_text': data['header']['header_text_example_field']}})
            else:
                lg.info("Static text variable with no variable")
                lst_components_data.append({'type': 'HEADER', 'format': 'TEXT', 'text': data['header']['header_text']})
        elif data['header']['header_type'] == 'MEDIA':
            lg.info("Dynamic media header with variable")
            lst_components_data.append({'type': 'HEADER', 'format': data['header']['attachment_type'], 'example': {'header_handle': [data['header']['attachment_header_handle']]}})
            # header_handle = [data['header']['attachment_header_handle']]  
            # lg.info(f"header handle is {header_handle}")                   
        else:
            header_handle = " "
            pass
        
        #footer
        if data['footer']['footer_text'] != "":
            lg.info("Footer")
            lst_components_data.append({"type": "FOOTER", "text": data['footer']['footer_text']})
        
        #button
        lst_button = []
        if 'button' in data:
            if data['button']['type'] != "None":
                if data['button']['type'] == 'callToAction':
                    for button_data in data['button']['call_to_action']:
                        if 'call_to_action_type' in button_data:
                            if button_data['type'] == 'callToAction' and button_data['call_to_action_type'] == 'URL':
                                if button_data['dynamic_url'] != '':
                                    lg.info("Dynamic button url with variable")
                                    lst_button.append({'type': 'URL', 'text': button_data['text'], 'url': button_data['static_url'] + "/" + "{{1}}", 'example': button_data['static_url'] + button_data['dynamic_url']})
                                    lg.info(f"value of lst_button after appending dynamic_url data is {lst_button}")
                                else:
                                    lg.info("Static button url with no variable")
                                    lst_button.append({'type': 'URL', 'text': button_data['text'], 'url': button_data['static_url']})
                            if button_data['type'] == 'callToAction' and button_data['call_to_action_type'] == 'PHONE_NUMBER':
                                lg.info("Phone number in button")
                                lst_button.append({'type': 'PHONE_NUMBER', 'text': button_data['text'], 'phone_number': button_data['phone_number']})                    
    
                if data['button']['type'] == 'quickReply':
                    for qr_data in data['button']['quick_reply_list']:
                        if qr_data["payload_type"] == "journey":
                            lg.info("Quick reply button with journey")
                            lst_button.append({"type": "QUICK_REPLY", "text": qr_data["text"]})
                        else:
                            lg.info("Quick reply button without journey")
                            lst_button.append({"type": "QUICK_REPLY", "text": qr_data["text"]})
                
                if data['button']['type'] == 'flow':
                        url = f"{base_url}/{waba_id}/flows"
                        lg.info(f"base url is {base_url} and waba_id is {waba_id} and flow_api is {url}")
                        headers = {"Authorization": f"Bearer {access_token}"}
                        response = requests.get(url, headers=headers).json()
                        lg.info(f"response of flow_API is {response}")
                        flow_data = next((flow for flow in response['data'] if flow['name'] == flow_name), None)
                        lg.info(f"flow_data for selected flow is {flow_data}")
                        flow_id = flow_data['id']
                        text = data['button']['flow_data'][0].get('text')
                        lg.info(f"flow_data_text is {text}")
                        lg.info(f"flow_id is {flow_id}")
                        lst_button.append({"type": "FLOW", "text": text,"flow_id":flow_id,"navigate_screen": "QUESTION_ONE","flow_action": "navigate"})
                        lg.info(f"lst_button data is {lst_button}")
                
                lst_components_data.append({'type': 'BUTTONS', 'buttons': lst_button})
                
        dict_body_data.update({"components": lst_components_data})
        lg.info(f"dict_body_data is {dict_body_data}")
        return dict_body_data

    
    def func_get_template_data(self,client_id,template_name,lg):
        """ Method called to get template by name """
        try:
            db_wa_system_account = self.ew_db.wa_system_account.find_one({},{"id":0})
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"ew_id":client_id}, {"id":0})
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings, wa_system_account : " + str(e))    
            return jsonify({"id": "1082", "message": "Data Query Error", "description": "", "data": "", "success": False})
        
        if db_client_waba_settings == None:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings : None")    
            return jsonify({"id": "1083", "message": "Invalid user input", "description": "Invalid client ID", "data": "", "success": False})
        
        var_url = graph_url + str(db_client_waba_settings['waba_id']) + "/message_templates?access_token=" + str(db_wa_system_account['system_user_token']) + "&name=" + str(template_name)
        var_payload = {}
        var_headers = {}
        var_response = requests.request("GET", var_url, headers=var_headers, data=var_payload)
        var_response_text = var_response.json()
        if var_response_text['data'] == []:
            lg.critical("ew_id=" + str(client_id) + " | " + "Error in api response for get template : " + str(var_response.json()))    
            return jsonify({"id": "1084", "message": "Template does not exist. Please, enter appropriate template name", "description": "", "data": "", "success": False})
        else:
            return jsonify({"id": "1085", "message": "Template fetched succussfully", "description": "", "data":json.loads(var_response.text), "success": True})


    def template_uniqueness(self,client_id,template_name,lg):
        """ Method called to check uniqueness of template """
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"ew_id":client_id}, {"_id":0})
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_waba_settings : " + str(e))    
            return jsonify({"id": "1092", "message": "Data Query Error", "description": "", "data": "", "success": False})

        var_url = graph_url + str(db_client_waba_settings['waba_id']) + "/message_templates?access_token=" + str(db_client_waba_settings['system_user_token'])
        var_payload = {}
        var_headers = {}
        var_response = requests.request("GET", var_url, headers=var_headers, data=var_payload)
        var_response_text = var_response.json()
        lst_template = var_response_text['data']
        lst_result = []
        for template in lst_template:
            lst_result.append(template['name'])

        if template_name in lst_result:
            return jsonify({"id": "1093", "message": "Template name already exist. Please, choose different name", "description": "", "data": "", "success": False})
        else:
            return jsonify({"id": "1094", "message": "Data fetched successfully", "description": "", "data":{"template name": template_name}, "success": True})


    def func_list_language_catagory(self,lg):
        lst_languages = ["Afrikaans", "Albanian", "Arabic", "Azerbaijani", "Bengali", "Bulgarian", "Catalan", 
        "Chinese (CHN)", "Chinese (HKG)", "Chinese (TAI)", "Croatian", "Czech", "Danish", "Dutch", "English",
        "English (UK)", "English (US)", "Estonian", "Filipino", "Finnish", "French", "German", "Greek", "Gujarati",
        "Hausa", "Hebrew", "Hindi", "Hungarian", "Indonesian", "Irish", "Italian", "Japanese", "Kannada", "Kazakh",
        "Korean", "Lao", "Latvian", "Lithuanian", "Macedonian", "Malay", "Malayalam", "Marathi", "Norwegian",
        "Persian", "Polish", "Portuguese (BR)", "Portuguese (POR)", "Punjabi", "Romanian", "Russian", "Serbian",
        "Slovak", "Slovenian", "Spanish", "Spanish (ARG)", "Spanish (SPA)", "Spanish (MEX)", "Swahili", "Swedish",
        "Tamil", "Telugu", "Thai", "Turkish", "Ukrainian", "Urdu", "Uzbek", "Vietnamese", "Zulu"]

        lst_category = ["Account Update", "Alert Update", "Appointment Update",
        "Auto-Reply", "Issue Resolution", "Payment Update", "Personal Finance Update",
        "Reservation Update", "Shipping Update", "Ticket Update", "Transportation Update"]

        response = {"language": lst_languages, "category": lst_category}
        return {"id": "1102", "message": "Data fetched successfully", "description": "", "data": response, "success": True}


    @phone_access_required
    def func_delete_template(self,str_client_id,str_template_name,str_email_id,lg):
        """ Method called to delete template """
        try:
            db_wa_system_account = self.ew_db.wa_system_account.find_one({},{"_id":0})
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"ew_id": str_client_id},{"_id":0})
            db_template_mapping = self.cl_db.template_mapping.find_one({"template_name": str_template_name, "ew_id": str_client_id, "$or": [{"created_by": str_email_id, "template_access": "PRIVATE"}, {"template_access": "PUBLIC"}]}, {"_id": 0})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - client_waba_settings, template_mapping, wa_system_account : " + str(e))    
            return jsonify({"id": "1114", "message": "Data Query Error", "description": "", "data": "", "success": False})
        
        if db_template_mapping == None:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : " + str(e))    
            return jsonify({"id":"1115", "message": "Invalid user input", "description": "Template doesnt exist in account or this user don't have permission to access this template", "data": "", "success": False})
        
        lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : ")    

        var_url = graph_url + str(db_client_waba_settings['waba_id']) + '/message_templates?name=' + str(str_template_name) + "&access_token=" + str(db_wa_system_account['system_user_token'])
        # url = https://graph.facebook.com/v16.0/waba_id123/message_templates?name=temp1&access_token=123456
        var_payload = {}
        var_headers = {}
        var_response = requests.request("DELETE", var_url, headers=var_headers, data=var_payload)
        if 'error' not in var_response.json():
            try:
                self.cl_db.template_mapping.update_one({"template_name": str_template_name, "ew_id": str_client_id}, {"$set": {"delete_timestamp": int(datetime.datetime.now().timestamp()), "template_delete": True}})
            except Exception as e:
                lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : " + str(e))    
                return jsonify({"id": "1116", "message": "Data Query Error", "description": "", "data": "", "success": False})
            
            return jsonify({"id": "1117", "message": "Template deleted successfully", "description": "", "data": "", "success": True})
        else:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "Error in WA delete template api : " + str(var_response.json()))    
            return jsonify({"id": "1118", "message": "The message template " + str(str_template_name) + " wasn't found for this account", "description": "", "data": "", "success": False})


    @phone_access_required
    def func_create_excelsheet(self,data,str_client_id,str_email_id,lg):
        try:
            db_template_mapping = self.cl_db.template_mapping.find_one({"template_name": data["template_name"], "ew_id": str_client_id, "$or": [{"created_by": str_email_id, "template_access": "PRIVATE"}, {"template_access": "PUBLIC"}]}, {"_id": 0})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : " + str(e))    
            return jsonify({"id": "1124", "message": "Data Query Error", "description": "", "data": "", "success": False})
        
        lg.info(f"db_template_setting value is {db_template_mapping}")
        if db_template_mapping != None:
            lst_result_data = []
            lst_result_data.append("MOBILE_NUMBER")
            lst_result_data.append("TEMPLATE_ID")
            lst_result_data.append("TEMPLATE_NAME")

            media_option = db_template_mapping.get("media_option", "")

            lg.info(f"value of media_option is {media_option}")

            if db_template_mapping["header"]["header_text_column_name"] != "":
                lst_result_data.append(db_template_mapping["header"]["header_text_column_name"])
            
            if db_template_mapping["header"]["attachment_column_name"] != "" and media_option == "on":
                lst_result_data.append(db_template_mapping["header"]["attachment_column_name"])

            if db_template_mapping["body"]["body_column_names"] != []:
                lst_body_column_names = db_template_mapping["body"]["body_column_names"]
                for body_data in lst_body_column_names:
                    lst_result_data.append(body_data)

            if "button" in db_template_mapping.keys():
                if "button1" in db_template_mapping["button"]:
                    if "dynamic_url_column_name" in db_template_mapping["button"]["button1"]:
                        if db_template_mapping["button"]["button1"]["dynamic_url_column_name"] != "":
                            lst_result_data.append(db_template_mapping["button"]["button1"]["dynamic_url_column_name"])
                    
                if  "button2" in db_template_mapping["button"]:
                    if "dynamic_url_column_name" in db_template_mapping["button"]["button2"]:
                        if db_template_mapping["button"]["button2"]["dynamic_url_column_name"] != "":
                            lst_result_data.append(db_template_mapping["button"]["button2"]["dynamic_url_column_name"])
                
            lst_response = []    
            for str_column_name in lst_result_data:
                lst_response.append(str_column_name.upper())
            return jsonify({"id": "1125", "message": "Data fetched successfully", "description": "", "data": lst_response, "success": True})
        else:
            return jsonify({"id":"1126", "message": "Invalid user input", "description": "Template doesnt exist in account or this user don't have permission to access this template", "data": "", "success": False})


    @phone_access_required
    def func_get_template_names(self,str_client_id,str_email_id,lg):
        try:
            db_template_mapping = list(self.cl_db.template_mapping.find({"template_delete": {"$exists": False}, "ew_id": str_client_id, "$or": [{"created_by": str_email_id, "template_access": "PRIVATE"}, {"template_access": "PUBLIC"}]}, {"_id":0, "template_status":1, "template_name":1, "template_id":1, "category":1, "language":1, "template_delete":1, "reason":1, "created_by":1, "template_access":1 , "timestamp" : 1}).sort("timestamp", -1))
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : " + str(e))    
            return jsonify({"id": "1134", "message": "Data Query Error", "description": "", "data": "", "success": False})
        
        lst_response = []
        for template in db_template_mapping:
            if template["template_status"] == "REJECTED":
                template_status = "REJECTED"
                reason = str(template["reason"])
            else:
                template_status = template["template_status"]
                reason = ""
            
            if template["created_by"] == str_email_id:
                owned_by = "SELF"
            else:
                owned_by = "OTHER USER"
            template.update({"template_status": template_status, "reason": reason, "owned_by": owned_by})
            lst_response.append(template)
        return jsonify({"id": "1135", "message": "Templates fetched successfully", "description": "", "data": lst_response, "success": True})


    @phone_access_required
    def func_get_template_by_name(self,str_client_id,str_email_id,data,lg):
        try:
            db_template_mapping = self.cl_db.template_mapping.find_one({"template_delete": {"$exists": False}, "ew_id": str_client_id, "$or": [{"created_by": str_email_id, "template_access": "PRIVATE"}, {"template_access": "PUBLIC"}], "template_name": data["template_name"]}, {"_id":0,"media_option":0})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : " + str(e))    
            return jsonify({"id": "1334", "message": "Data Query Error", "description": "", "data": "", "success": False})
        if db_template_mapping == None:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : None")    
            return jsonify({"id": "1335", "message": "Template does not exist", "description": "", "data": "", "success": False})
        
        return jsonify({"id": "1336", "message": "Templates fetched successfully", "description": "", "data": db_template_mapping, "success": True})
    
    def func_get_template_by_name_livechat(self,str_client_id,data,lg):
        try:
            db_template_mapping = self.cl_db.template_mapping.find_one({"template_delete": {"$exists": False}, "ew_id": str_client_id, "$or": [{ "template_access": "PRIVATE"}, {"template_access": "PUBLIC"}], "template_name": data["template_name"]}, {"_id":0,"media_option":0})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : " + str(e))    
            return jsonify({"id": "1334", "message": "Data Query Error", "description": "", "data": "", "success": False})
        if db_template_mapping == None:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : None")    
            return jsonify({"id": "1335", "message": "Template does not exist", "description": "", "data": "", "success": False})
        
        return jsonify({"id": "1336", "message": "Templates fetched successfully", "description": "", "data": db_template_mapping, "success": True})


    def func_get_template_by_name_for_edit_template(self,str_client_id,str_email_id,data,lg):
        try:
            db_template_mapping = self.cl_db.template_mapping.find_one({"template_delete": {"$exists": False}, "ew_id": str_client_id, "$or": [{"created_by": str_email_id, "template_access": "PRIVATE"}, {"template_access": "PUBLIC"}], "template_name": data["template_name"], "template_id": int(data["template_id"])}, {"_id":0,"media_option":0})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : " + str(e))    
            return jsonify({"id": "1334", "message": "Data Query Error", "description": "", "data": "", "success": False})
        if db_template_mapping == None:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : None")    
            return jsonify({"id": "1335", "message": "Template does not exist", "description": "", "data": "", "success": False})
        else:
            call_to_action_list = []
            quick_reply_list = []
            if "button" in db_template_mapping:
                if "button1" in db_template_mapping["button"]:
                    if db_template_mapping["button"]["button1"]["type"] == "callToAction":
                        call_to_action_list.append(db_template_mapping["button"]["button1"])
                    if db_template_mapping["button"]["button1"]["type"] == "quick_reply":
                        qr_list = []
                        for single_quick_reply in db_template_mapping["button"]["button1"]["quick_reply_list"]:
                            new_qr_payload = single_quick_reply["payload"]
                            import ast
                            new_qr_json = ast.literal_eval(new_qr_payload)
                            lg.info(new_qr_json)
                            qr_list.append({"type": "quickReply", "text": single_quick_reply["text"], "journey_name": new_qr_json["J"], "payload_type": single_quick_reply["payload_type"]})
                        quick_reply_list = {"quick_reply_list": qr_list, "type": "quickReply"}
                if "button2" in db_template_mapping["button"]:
                    if db_template_mapping["button"]["button2"]["type"] == "callToAction":
                        call_to_action_list.append(db_template_mapping["button"]["button2"])

            if call_to_action_list != []:
                button_dict = {"type": "callToAction", "call_to_action": call_to_action_list}
                db_template_mapping["button"] = button_dict
            if quick_reply_list != []:
                button_dict = quick_reply_list
                db_template_mapping["button"] = button_dict

        return jsonify({"id": "1336", "message": "Templates fetched successfully", "description": "", "data": db_template_mapping, "success": True})


    @phone_access_required
    def func_change_template_access(self,str_client_id,str_email_id,data,lg):
        try:
            db_template_mapping = self.cl_db.template_mapping.update_one({"ew_id": str_client_id, "template_name": data["template_name"], "template_id": int(data["template_id"]), "created_by": str_email_id}, {"$set": {"template_access": data["template_access"]}})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "DB error - template_mapping : " + str(e))    
            return jsonify({"id": "1164", "message": "Data Query Error", "description": "", "data": "", "success": False})

        return jsonify({"id": "1165", "message": "Template access changed successfully", "description": "", "data": "", "success": True})


    def func_list_wa_journey(self):
        var_url = url_for_dmp + "/whatsapp_journey_list"
        var_headers = {}
        var_payload = {"int_id": self.db_client_waba_settings["response"]["int_id"]}
        var_response = requests.post(var_url, headers=var_headers, json=var_payload)
        return jsonify({"id": "1144", "message": "Journey data fetched successfully", "description": "", "data": json.loads(var_response.content.decode("utf-8"))["data"], "success": json.loads(var_response.content.decode("utf-8"))["success"]})


    def func_quick_reply_type(self,client_id,lg):
        try:
            db_quick_reply_button_type = self.ew_db.quick_reply_button_type.find_one({},{"_id":0})
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - quick_reply_button_type : " + str(e))    
            return jsonify({"id": "1152", "message": "Data Query Error", "description": "", "data": "", "success": False})
        return jsonify({"id": "1153", "message": "Data fetched successfully", "description": "", "data": db_quick_reply_button_type, "success": True})

# class ClsTemplateOperations2():
#         def func_update_template_status(self,client_id,lg):
#             try:
#                 db_wa_system_account = self.ew_db.wa_system_account.find_one({},{"_id":0})
#                 temp_data = list(self.cl_db.template_mapping.find({"template_status":"PENDING"},{"_id":0,"template_name":1}))
#                 db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"ew_id":client_id}, {"_id":0})
#                 if temp_data != []:
#                     for temp in temp_data:
#                         url = graph_url + str(db_client_waba_settings['waba_id']) + "/message_templates?name=" + str(temp["template_name"] )
#                         payload = {}
#                         headers = {'Authorization': 'Bearer '+str(db_wa_system_account["system_user_token"])}
#                         response = requests.request("GET", url, headers=headers, data=payload)      
#                         graph_data = response.json()    
#                         lg.critical("ew_id=" + str(client_id) + " | " + " Graph_data = " + str(graph_data))    
#                         for records in graph_data["data"]:
#                             if records["name"] == temp["template_name"]:
#                                 status = records["status"]
#                                 if status == "PENDING":
#                                     pass
#                                 else:
#                                     lg.info("ew_id=" + str(client_id) + " | " + ": " + str(e))

#                                     self.cl_db.template_mapping.update_one({"template_name":temp["template_name"]},{"$set":{"template_status":status}})
#                             else:
#                                 pass
#                 else:
#                     pass 
#                 return jsonify({"message":"Templates status updated successfully"}) 
#             except Exception as e:  
#                 lg.critical("ew_id=" + str(client_id) + " | " + "DB error - template_mapping : " + str(e))    
#                 return jsonify({"message":"Please check the template status"}) 

class ClsTemplatestatus():
    """ Class called to perform temprate actions """

    def __init__(self):
        self.ew_db = ClsMongoDBInit.get_ew_db_client()            

            
    def updated_template_status(self,lg):
        db_client_waba_settings = list(self.ew_db.client_waba_settings.find({},{"_id":0,"waba_id":1,"ew_id":1}))
        db_system_token = list(self.ew_db.wa_system_account.find({}))
        sytem_token = db_system_token[0]["system_user_token"]
        client_db = []
        for db in db_client_waba_settings:
            client_db_name = db["ew_id"].lower() + "_" + str(db["waba_id"])
            client_db.append(client_db_name)
        final_client_db_name = list(set(client_db))
        for db_data in final_client_db_name:
            waba_id = db_data.split("_")[1]
            ew_id = db_data.split("_")[0]
            client_db = ClsMongoDBInit.get_cl_db_client(db_data)
            cl_db = list(client_db.template_mapping.find({"template_status":"PENDING"},{"_id":0,"template_name":1}))
            if cl_db != []:
                for temp in cl_db:
                    url = graph_url + str(waba_id) + "/message_templates?name=" + str(temp["template_name"])
                    payload = {}
                    headers = {'Authorization': 'Bearer '+sytem_token}
                    response = requests.request("GET", url, headers=headers, data=payload)              
                    graph_data = response.json()
                    for records in graph_data["data"]:
                        if records["name"] == temp["template_name"]:
                            status = records["status"]
                            if status == "PENDING":
                                pass
                            else:
                                client_db.template_mapping.update_one({"template_name":temp["template_name"]},{"$set":{"template_status":status}})
                                lg.info("ew_id: " + str(ew_id) + ", Template name: " +temp["template_name"] + "status: "+ status)
        return jsonify({"message":"Templates status updated successfully"})   