
from flask import jsonify
import requests
import pandas as pd
import os
import json
import datetime
from urllib.parse import urlparse
import time
import numpy as np
import math
import copy

# Import custom packages
from waapi.celery_worker2 import async_broadcast_single_template  # ---- celery change ----


from wacore.pkg_db_connect.mod_db_connection import ClsMongoDBInit
from wacore.pkg_analytics.mod_analytics_function import ClsBroadcastAnalysis
from wacore.pkg_extras.mod_common import ClsCommon
from walogger.walogger import WaLogger


from ..pkg_concent_management import mod_concent_management_functions


# Initialize logger with name that we want or any other
obj_log = WaLogger('pktmp')
lg = obj_log.get_logger()
obj_common = ClsCommon()

class ClsCeleryBroadcast():
    """ Class calling for celery broadcast """
    
    def __init__(self,lst_columns_db,excel_file,dict_payload):
        """ Create or initialize object and variables """
        self.ew_id = dict_payload["ew_id"]
        self.client_number = dict_payload["client_number"]
        self.template_name = dict_payload["template_name"]
        self.broadcast_id = dict_payload["broadcast_id"]
        self.broadcast_name = dict_payload["broadcast_name"]
        self.client_db_name = dict_payload["client_db_name"]
        self.db_template_mapping = dict_payload["db_template_mapping"]
        self.language_code = dict_payload["language"]
        self.lst_columns_db = lst_columns_db
        try:
            self.db_client_waba_settings = obj_common.get_waba_settings_by_cc_client_number(self.client_number)
        except:
            return jsonify({"error": {"id": "2003", "message": "Invalid client number in job", "success": False}})
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        self.cl_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)
        self.contact_collection_name = "contacts_" + self.client_number
        self.df = pd.DataFrame(excel_file)
        # self.df_init = pd.read_excel(excel_file_lst)
        # self.df = pd.read_excel(excel_file)
        # col_name = self.df.columns[3]
        # if self.df[col_name].isna().any():
        #         lg.info("nan values found inside mod celery")
        # else:
        #     lg.info("not found Nan inside mod celery")
        self.verify_contact_with_FB = True

        obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Broadcast template sending initialized")
        
        
    def fun_get_contact_status(self,email_id,consen_header):

        # # ---------- Extract valid invalids start ----------
        # lst_contact_excel = list(self.df["MOBILE_NUMBER"]) # Extract All Mobile from DF
        # # print(">"*55)
        # # print(lst_contact_excel)
        # lst_contact_space_clean = []
        # lst_invalid_contact_not_int = []
        # temp_contact = ""
        # for contact in lst_contact_excel:
        #     try:
        #         temp_contact = str(contact).replace(" ","")
        #         int_contact_verify = int(temp_contact)
        #         lst_contact_space_clean.append(temp_contact)
        #     except:
        #         lst_invalid_contact_not_int.append(temp_contact)

       
        # chunk_steps = 500 # large list is divided in to smaller list for contact verification API of Facebook
        # lst_chunk = [list(set(lst_contact_space_clean))[i * chunk_steps:(i + 1) * chunk_steps] for i in range((len(set(lst_contact_space_clean)) + chunk_steps - 1) // chunk_steps)]

        
        # lst_total_valid_number = []
        # for i in range(0,len(lst_chunk)):
        #     meta_status = self.fun_verify_new_contacts(list(map(lambda x:"+"+str(x), lst_chunk[i])))
        #     try:
        #         tpl_valid_number,lst_result = zip(*[(str(x['wa_id']),{'recipient_number': int(x['wa_id']),"expiry": int((datetime.datetime.now() + datetime.timedelta(days=7)).timestamp())})  for x in meta_status["contacts"] if x["status"] == "valid"])
        #         lst_total_valid_number = lst_total_valid_number + list(tpl_valid_number)
        #         self.verify_contact_with_FB = True
        #     except:
        #         tpl_valid_number=set()
        #         lst_result=[]

        # invalid_by_fb = set(lst_contact_space_clean)-set(lst_total_valid_number) # 2-INVD= should mark as invalid by FB numbers
        # final_invalid_contacts = set(lst_invalid_contact_not_int).union(invalid_by_fb)
        # # print("Invalid by FB=")
        # # print(invalid_by_fb)
        # # print("TOTAL Invalid numbers=")
        # # print(final_invalid_contacts)

        # if self.verify_contact_with_FB == False:
        #     obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Failed to verify contacts with FB.")
        # else:
        #     obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Contacts verification successful.")
        # # ---------- Extract valid invalids End ----------

        # ---------- Generate Single Payload & Send Message Start ---------- 
        lg.info("inside celery 1")
        final_invalid_contacts=[]
        consen_header = consen_header
        lg.info(f"consen_header inside fun_get_contact_status function is {consen_header} ")     
        single_json_payload = self.func_create_json(self.db_template_mapping)
        lg.info(f"single_json_payload is {single_json_payload}")
        response_of_submission = self.func_send_message_template(final_invalid_contacts,single_json_payload,email_id,consen_header)
        # ---------- Generate Single Payload & Send Message End ----------    
        return {"message":"Contact Verified Successfully", "result": True}

    #### Not used after version 2.45.x, all contacts are valid return by default - START
    # def fun_verify_new_contacts(self,lst_new_contacts_from_xl):
    #     """ 
    #     Function called for processing invalid contact status 
    #     """
    #     try:
    #         url = self.db_client_waba_settings["response"]["url"] + "contacts"
    #         token = self.db_client_waba_settings["response"]["access_token"]
    #         payload= {'blocking': 'wait', 'contacts': lst_new_contacts_from_xl} # 'force_check': True   ...is removed
    #         headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
    #         response = requests.request('POST', url, headers=headers, data=json.dumps(payload))
    #         return response.json()
    #     except Exception as e:
    #         self.verify_contact_with_FB = False
    #         return {"message":"Contact Verification API Failed", "result": False}
    #### Not used after version 2.45.x, all contacts are valid return by default- END

    def func_create_json(self,db_template_mapping):
        """ Function called for create json for template """ 
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
                    if db_template_mapping['header']['attachment_type'] == "location":
                         var_header = {'header': {'type': db_template_mapping['header']['attachment_type'],  db_template_mapping['header']['attachment_type']: "samplelocation"}}
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
                    # else:
                    #     button_data = None
            else:
                var_button = None
            
            if var_header != None:
                dict_components.update(var_header)
            
            if var_button != None:
                dict_components.update(var_button)
            
            dict_components.update(body_data)

            resp_json = {"api": "WA", "waid": "1d4aa0-1203-4c85-8dc3", "version": "v1", "type": "template", "template_name": db_template_mapping["template_name"], "payload": {"from": "sample_sender_contact", "to": "sample_recipient_contact", "components": dict_components}}
            obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Message payload created successfully, sending started.")
            return resp_json
        except:
            obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Failed to create message payload.")
            lg.critical("Failed to create message payload.EXCEPT:Key Error in create_json().")
            return "Failed to create message payload." 
        

    def add_data_to_concent_collection(self, payload,consen_header):
        try:
            url = "https://whatsappapi.engagely.ai/waapi/concent/add_recipients"

            payload = json.dumps(payload)
            lg.info(f"payload before calling api is {payload}")
            headers = consen_header

            # try three times if api fails, with delay of 10 seconds
            for x in range(1,4):
                response = requests.request("POST", url, headers=headers, data=payload)

                if response.status_code not in [200, 400]:
                    time.sleep(10*x)
                else:
                    break

                if x == 3:
                    lg.info(f"ERROR: {url} called three times but failed with status code:{response.status_code} and response: {response.text}")


            return response.text

        except Exception as e:
            lg.info(f"ERROR: concent insertion api failed -> {e}")

            return f"ERROR: {e}"
    

    def func_send_message_template(self,final_invalid_contacts,dict_message_data_new,email_id,consen_header):
        """
        Function called to send message template in celery task

        Parameteres:
            self.df: Original Excel DataFrame
            final_invalid_contacts: List of invalid numbers
            dict_message_data: Single sample JSON payload

        Returns:
            Start Sending Single Messages and return response from DB
        """
        # Update Total Recepient
        lg.info("ew_id=" + str(self.ew_id) + " | " + "df index : " + str(len(self.df.index)))    
        self.cl_db.broadcast_details.update_one({"ew_id": self.ew_id, "client_number": self.client_number, "broadcast_id": self.broadcast_id, "broadcast_name": self.broadcast_name},{"$set": {"recipient_total_count": len(self.df.index),"cost":0.80*len(self.df.index)}})
        # self.cl_db.broadcast_details.insert_one( {"ew_id": self.ew_id, "client_number": self.client_number, "broadcast_id": self.broadcast_id, "broadcast_name": self.broadcast_name, "template_name": self.template_name, "status": "SUBMITTED", "recipient_total_count": len(self.df.index), "recipient_sent_count": 0, "recipient_delivered_count": 0, "recipient_read_count": 0, "recipient_failed_count": 0, "timestamp": int(datetime.datetime.now().timestamp()), "success_stats":[], "failed_stats":[], "upload_excel_url" : "", "created_by": email_id})
        df_excel_columns_names = self.df.columns.tolist()
        str_recipient_number = dict(dict_message_data_new).get("payload")
        total_mobile_count = int(len(self.df.index)) - 1
        if str_recipient_number is not None:
            
            recipient_number = dict(str_recipient_number).get("to")

        # adding data to concent db
        mobile_number_list = self.df["MOBILE_NUMBER"].to_list()
        try:    
            # concent_add_payload = {"recipient_number": mobile_number_list}
            # consen_header = consen_header
            # lg.info(f"concent_add_payload is {concent_add_payload},{consen_header} ")
            # concent_add_response = self.add_data_to_concent_collection(concent_add_payload,consen_header)

            # lg.info(f"concent add data api request: {concent_add_response}")


            # fetch numbers where concent flag is false
            # obj = mod_concent_management_functions.ClsConcentManagementFunc(self.cl_db)

            # number_with_false_concent_list = obj.number_with_concent_false()
            query = {'concent_flag': False}

            number_with_false_concent_list= list(self.cl_db.concent_details.find(query, {'_id': 0,"recipient_number":1}))
            
            number_with_false_concent_list =  [item['recipient_number'] for item in number_with_false_concent_list]

            lg.info(f"NUmbers with consent False is {number_with_false_concent_list}")

            lg.info(f"NUmbers with consent False is {number_with_false_concent_list}")
        except Exception as e:
            number_with_false_concent_list = []
            lg.info("consent management is not added for this client")

        # var_invalid_contact_list1 = list(map(str, final_invalid_contacts))
        d=[]
        for var_row in range(int(len(self.df.index))):
            dict_message_data =copy.deepcopy(dict_message_data_new)
            lst_single_row = list(self.df.iloc[var_row].values.tolist())
            # lst_single_row = list(self.df_init.iloc[var_row].values.tolist())
            # lst_single_row = list(self.df.iloc[var_row, :].to_numpy())
            recpt_number = str(lst_single_row[0]).replace(" ","")
            lg.info(number_with_false_concent_list)
            # if concent is false skip it
            if number_with_false_concent_list != []:
                try:
                    lg.info(f"Number we are checking {recpt_number}")
                    if int(recpt_number) in number_with_false_concent_list:
                        print(f"{recpt_number} is false, skipping it")
                        lg.info(f"{recpt_number} is false, skipping it")
                        self.cl_db.broadcast_details.update_one({"broadcast_id": self.broadcast_id}, {"$inc":{"recipient_failed_count":1} })
                        self.cl_db.broadcast_details_info.insert_one({"broadcast_id": self.broadcast_id, "recipient_number": recpt_number, "reason": "Consent is False", "dynamic_variables" : "", "errors": "Consent is False"})
                        # self.cl_db.broadcast_details.update_one({"broadcast_id": self.broadcast_id}, {"$push": {"failed_stats": {"recipient_number": recpt_number, "reason": "Consent is False", "dynamic_variables" : "", "errors": "Consent is False"}}, "$inc":{"recipient_failed_count":1} })
                        continue
                except Exception as e:
                    lg.info(f"Error is {e}")

            analytics_arguments = {"broadcast_id": self.broadcast_id,"broadcast_name": self.broadcast_name, "recipient_number": recpt_number}
            if recpt_number not in final_invalid_contacts:
                
                # header
                try:
                    lg.info(f"dict message {dict_message_data}ddd {final_invalid_contacts}sdsd {lst_single_row}")
                    dict_message_data["payload"]["from"] = self.client_number
                    dict_message_data["payload"]["to"] = recpt_number
                except:
                    pass

                if self.db_template_mapping["header"]["header_type"] == "TEXT" or self.db_template_mapping["header"]["header_type"] == "MEDIA":
                    if self.db_template_mapping["header"]['attachment_column_name'] != "":
                        try:
                            '''
                            #nan logic commented
                            col_name = self.df.columns[3]
                            if self.df[col_name].isna():
                                lg.info("nan values found at line 300")
                            else:
                                lg.info("no nan values found at line 302")
                            # col_name = self.df.columns[3]
                            # lst_link = self.df[col_name].tolist()
                            # dict_message_data["payload"]["components"]["header"]["link"] = lst_link[var_row]
                            image_url = lst_single_row[3]
                            lg.info(f"image_url value is {image_url}")
                            if math.isnan(image_url):
                                lg.info(f"inside first if for nan")
                                mobile_number_li = lst_single_row[0]
                                lg.info(f"mobile_number is {mobile_number_li}")
                                if  mobile_number_li in self.df['MOBILE_NUMBER'].tolist():
                                    lg.info("inside second if of nan")
                                    col_index = 3
                                    row_number  = self.df.index[self.df['MOBILE_NUMBER'] == mobile_number_li].tolist()
                                    row_number = int(row_number[0])
                                    lg.info(f"row_number is {row_number}")
                                    #  replacement_link = df.iloc[row_number, column_index]
                                    try:
                                        replacement_link = self.df.iloc[row_number, col_index]                                    
                                        lg.info(f"replacement_link is {replacement_link}")
                                        image_url = replacement_link
                                        lg.info(f"updated image_url is {image_url}")
                                    except Exception as e:
                                        lg.info(f"error inside nan is {e}")
                            else:
                                dict_message_data["payload"]["components"]["header"]["link"] = image_url

                            '''
                            #original code commented
                            if dict_message_data["payload"]["components"]["header"]["type"]=="location":
                                location_data = str(lst_single_row[3]).split(":")
                                dict_message_data["payload"]["components"]["header"]["location"] =  {"latitude": str(location_data[0]),"longitude": str(location_data[1]),"name":str(location_data[2]),"address": str(location_data[3])}
                                hd = True

                            else:
                                dict_message_data["payload"]["components"]["header"]["link"] = lst_single_row[3]
                                lg.info("inside image_Link key ")
                                hd = True
                            # lg.info("Media in header")
                        except Exception as e:
                            hd = False
                            lg.info(f"error is {str(e)}")
                            # lg.info("No media in header")
                    if self.db_template_mapping["header"]['header_text_column_name'] != "":
                        try:
                            dict_message_data["payload"]["components"]["header"][str(df_excel_columns_names[3])] = lst_single_row[3]
                            hd = True
                            # lg.info("Dyanamic text in header")
                        except:
                            hd = False
                            # lg.info("No dynamic text in header")
                    if self.db_template_mapping["header"]['attachment_column_name'] == "" and self.db_template_mapping["header"]['header_text_column_name'] == "":
                        # lg.info("Static header")
                        hd = False
                else:
                    # lg.info("No header")
                    hd = False
                
                # body
                if self.db_template_mapping["body"] != "" :
                    int_body_length = len(self.db_template_mapping["body"]["body_column_names"])
                    int_body_length = int_body_length + 3
                    int_before_col_length_without_header = 3
                    int_before_col_length_with_header = 4
                    if hd == False:
                        try:
                            int_body_field_counter = 0
                            for i in range (int_before_col_length_without_header,int_body_length+1):
                                dict_message_data["payload"]["components"]["body"][int_body_field_counter][str(df_excel_columns_names[i])] = str(lst_single_row[i])
                                int_body_field_counter = int_body_field_counter + 1
                            # lg.info("Dynamic body")
                        except:
                            # lg.info("Static body")
                            pass
                    else:
                        try:
                            int_body_field_counter = 0
                            for i in range (int_before_col_length_with_header, int_body_length+1):
                                dict_message_data["payload"]["components"]["body"][int_body_field_counter][str(df_excel_columns_names[i])] = str(lst_single_row[i])
                                int_body_field_counter = int_body_field_counter + 1
                            # lg.info("Dynamic body")
                        except:
                            # lg.info("Static body")
                            pass
                
                if "button" in self.db_template_mapping:
                    try:
                        dict_message_data["payload"]["components"]["button"]["url"] = str(lst_single_row[-1]).strip()
                        # lg.info("Dynamic url in button1")
                        pass
                    except:
                        # lg.info("Dynamic url not in button1")
                        pass
                    try:
                        dict_message_data["payload"]["components"]["button"]["url"] = str(lst_single_row[-1]).strip()
                        # lg.info("Dynamic url in button2")
                        pass
                    except:
                        # lg.info("Dynamic url not in button2")
                        pass
                lg.info("dev")
                lg.info(f"dict_message_data {dict_message_data}")
                d.append(dict_message_data)
                dict_message_data =""
            
            else:
                # -----------------(added for analytics)--------------
                str_recipient_number = analytics_arguments["recipient_number"]
                lg.info("Invalid number : " + str(str_recipient_number))    

                obj_clsbroadcastanalysis = ClsBroadcastAnalysis(self.client_number)
                obj_clsbroadcastanalysis.func_broadcast_stats_invalid(self.broadcast_id,str_recipient_number)
            
            # Change final status of brodcast to COMPLETED or FAILED
            
            if int(var_row) == int(total_mobile_count):
                time.sleep(5)
                # db_final_broadcast_details = self.cl_db.broadcast_details.find_one({"ew_id": self.ew_id, "client_number": self.client_number, "broadcast_id": self.broadcast_id, "broadcast_name": self.broadcast_name},{"recipient_sent_count":1,"_id":0})
                db_final_broadcast_details = self.cl_db.broadcast_details.find_one({"ew_id": self.ew_id, "client_number": self.client_number, "broadcast_id": self.broadcast_id, "broadcast_name": self.broadcast_name},{"recipient_sent_count":1, "success_stats":1, "_id":0})                
                lg.info("mobile count on final step : " + str(db_final_broadcast_details) + "   var row : " + str(var_row))
                if db_final_broadcast_details == None:
                    pass
                else:
                    # if int(db_final_broadcast_details["recipient_sent_count"]) == 0:
                    if db_final_broadcast_details["success_stats"] == []:
                        self.cl_db.broadcast_details.update_one({"ew_id": self.ew_id, "client_number": self.client_number, "broadcast_id": self.broadcast_id, "broadcast_name": self.broadcast_name},{"$set": {"status": "COMPLETED","finished_timestamp": int(datetime.datetime.now().timestamp()+ (5 * 3600) + (30 * 60))}})
                    else:
                        self.cl_db.broadcast_details.update_one({"ew_id": self.ew_id, "client_number": self.client_number, "broadcast_id": self.broadcast_id, "broadcast_name": self.broadcast_name},{"$set": {"status": "COMPLETED","finished_timestamp": int(datetime.datetime.now().timestamp()+ (5 * 3600) + (30 * 60))}})
        lg.info(f"d is for now {d}")
        var_data = self.func_template_message(d,analytics_arguments) #  analytics_arguments added for analytics
        obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Task finished, check Analytics for more information.")
        


        return var_data


    def func_template_message(self,var_message_data,var_analytics_arguments): #  analytics_arguments added for analytics
        """ Function called for adding functions create message request body and send message request """
        finaldata =[]
        lg.info(f"var_message_data {var_message_data}")
        for i in var_message_data:
            lg.info(f"i am i {i}")
            var_message = self.func_create_message_request_body(i)
            lg.info(f"i am var_message {var_message}")
            finaldata.append(var_message)
        lg.info(f"finaldata from create json {len(finaldata)}")
        # Prepare data for call
        str_url_templates = self.db_client_waba_settings['response']['url'] + self.db_client_waba_settings['response']['Phone-Number-ID']+"/messages"
        lg.info(f"meta url is {str_url_templates}")

        wa_token = self.db_client_waba_settings['response']['access_token']
        str_recipient_number = var_analytics_arguments["recipient_number"]
        lg.info("Before celery 2")
        finaltsk =[]
        chunk_size = 1000

        chunks = [finaldata[i:i + chunk_size] for i in range(0, len(finaldata), chunk_size)]
        lg.info(f"Time before processing the chunk of{datetime.datetime.utcnow()}")

        for chunk in chunks:
         lg.info(f"Processing chunk: {chunk}")
         var_message_send = async_broadcast_single_template.delay(chunk,
                                                              str_recipient_number,
                                                              str_url_templates,
                                                              wa_token,
                                                              self.client_number,
                                                              self.broadcast_id,
                                                              self.broadcast_name)

        lg.info(f"Time After processing the chunk of 1000{datetime.datetime.utcnow()}")
        remaining_chunk = finaldata[chunk_size * len(chunks):]
        if remaining_chunk:
          lg.info(f"Processing remaining items: {remaining_chunk}")
          var_message_send = async_broadcast_single_template.delay(remaining_chunk,
                                                              str_recipient_number,
                                                              str_url_templates,
                                                              wa_token,
                                                              self.client_number,
                                                              self.broadcast_id,
                                                              self.broadcast_name)
        # lg.info(f"Time After processing the chunk of 1000{datetime.datetime.utcnow()}")
#         for dvar in finaldata:
#             if len(finaltsk) < 1000:
#                 finaltsk.append(dvar)
#             else:

# #        dvar = np.array_split(d,math.ceil(len(d)/1000))
#                 lg.info(f"inside for loop {finaltsk}")
#                 var_message_send = async_broadcast_single_template.delay(finaltsk,
#                                                                     str_recipient_number,
#                                                                     str_url_templates,
#                                                                     wa_token,
#                                                                     self.client_number,
#                                                                     self.broadcast_id,
#                                                                     self.broadcast_name)#  analytics_arguments added for analytics
#                 finaltsk=[]
#         if len(finaltsk) > 0:
#             lg.info(f"inside for loop {finaltsk}")
#             var_message_send = async_broadcast_single_template.delay(finaltsk,
#                                                                 str_recipient_number,
#                                                                 str_url_templates,
#                                                                 wa_token,
#                                                                 self.client_number,
#                                                                 self.broadcast_id,
#                                                                 self.broadcast_name)#  analytics_arguments added for analytics
#             finaltsk=[]
        return var_message_send


    def func_create_message_request_body(self,var_message_data):
        """ Function called to create message request body """
        try:
            var_body = var_message_data['payload']['components']['body']
            lst_text = []
            for dict_body in var_body:
                dict_text_data = {"type": "text","text": list(dict_body.values())[0]}
                lst_text.append(dict_text_data)
            lst_component_data = [{"type": "body","parameters": lst_text}]

            if 'header' in var_message_data['payload']['components']:
                if var_message_data['payload']['components']['header']['type'] == 'text':
                    dict_header = var_message_data['payload']['components']['header']
                    header_text_keys = list(dict_header.keys())
                    header_text_values = list(dict_header.values())
                    if header_text_keys[0] == 'type':
                        str_header_text = header_text_values[1]
                    if header_text_keys[1] == 'type':
                        str_header_text = header_text_values[0]    
                    dict_header_data = {"type": "header", "parameters": [{"type": "text", "text": str_header_text}]}
                else:
                    if str(var_message_data['payload']['components']['header']['type']) == 'document':
                        filename = self.func_add_filename(var_message_data['payload']['components']['header']['link'])
                        dict_header_data = {"type": "header", "parameters": [{"type": var_message_data['payload']['components']['header']['type'], str(var_message_data['payload']['components']['header']['type']): {'link': var_message_data['payload']['components']['header']['link'], 'filename': filename}}]}             
                    elif str(var_message_data['payload']['components']['header']['type']) == 'location':
                            try:
                                # dict_header_data = {"type": "header", "parameters": [{"type": var_message_data['payload']['components']['header']['type'], str(var_message_data['payload']['components']['header']['type']): { var_message_data['payload']['components']['header']['location']}}]}
                                dict_header_data = {"type": "header", "parameters": [{"type": var_message_data['payload']['components']['header']['type'], str(var_message_data['payload']['components']['header']['type']): var_message_data['payload']['components']['header']['location']}]}
                                lg.info(f"dict_header_data is {dict_header_data}")
                            except Exception as e:
                                lg.info(f"error is {str(e)}")
                    else:
                        dict_header_data = {"type": "header", "parameters": [{"type": var_message_data['payload']['components']['header']['type'], str(var_message_data['payload']['components']['header']['type']): {'link': var_message_data['payload']['components']['header']['link']}}]}
            else: 
                dict_header_data = None
            
            if 'button' in var_message_data['payload']['components']:
                if 'button' in self.db_template_mapping:
                    if 'button1' in self.db_template_mapping['button'] and self.db_template_mapping['button']['button1']['call_to_action_type'] == 'URL':
                        int_index = 0
                    elif 'button2' in self.db_template_mapping['button'] and self.db_template_mapping['button']['button2']['call_to_action_type'] == 'URL':
                        int_index = 1
                else:
                    pass
                dict_button_data = {"type": "button", "sub_type" : "url", "index": int_index, "parameters": [{"type": "text", "text": var_message_data['payload']['components']['button']['url']}]}   
            else: 
                dict_button_data = None

            try:
                if 'button1' in self.db_template_mapping['button'] and self.db_template_mapping['button']['button1']['type'] == 'quick_reply':
                    dict_button_data = []
                    count = 0
                    for qr in self.db_template_mapping['button']['button1']['quick_reply_list']:
                        count = count + 1
                        dict_qr_data = {"type": "button", "sub_type": "quick_reply", "index": count-1, "parameters": [{"type": "payload", "payload": qr["payload"]}]}
                        dict_button_data.append(dict_qr_data)
            except:
                dict_button_data = None
            
            if dict_header_data != None:
                lst_component_data.append(dict_header_data)
            
            if dict_button_data != None:
                if isinstance(dict_button_data, list) == False:
                    lst_component_data.append(dict_button_data)
                else:
                    lst_component_data = lst_component_data + dict_button_data
            # var_data = json.dumps({"to": var_message_data['payload']['to'], "type": "template", "template": {"namespace":  self.db_client_waba_settings['response']['namespace'], "name": var_message_data['template_name'], "language": {"policy": "deterministic", "code": self.language_code}, "components": lst_component_data}})
            var_data = json.dumps({ "messaging_product": "whatsapp", "recipient_type": "individual","to": var_message_data['payload']['to'], "type": "template", "template": { "name": var_message_data['template_name'], "language": { "code": self.language_code}, "components": lst_component_data}})
            return var_data
        except:
            return "Key error"

    # ################# final sending logic
    # def func_send_message_request(self,var_message_body,var_analytics_arguments):#  analytics_arguments added for analytics
    #     """ Function called to send message request """
    #     str_url_templates = self.db_client_waba_settings['response']['url'] + "messages/"
    #     dict_headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.db_client_waba_settings['response']['access_token']}
    #     response_templates = requests.request("POST", str_url_templates, headers=dict_headers, data=var_message_body)
    #     resp = response_templates.json()

    #     #-------------Added for Analytics-------------------------
    #     str_recipient_number = var_analytics_arguments["recipient_number"]
    #     obj_clsbroadcastanalysis = ClsBroadcastAnalysis(self.client_number)
    #     obj_clsbroadcastanalysis.func_broadcast_stats_valid(self.broadcast_id,self.broadcast_name,resp,self.client_number,str_recipient_number)

    #     #------------------------Analytics End----------------------

    #     # if 'errors' in  response_templates.json():
    #     #     resp = response_templates.json()['errors'][0]['title']
    #     return resp


    def func_add_filename(self,media_link):
        obj_filename = urlparse(media_link)
        filename = str(os.path.basename(obj_filename.path))
        if len(filename) > 60:
            filename = filename[:59]
        return filename


    def __del__(self):
        self.ew_db.client.close()
        self.cl_db.client.close()
