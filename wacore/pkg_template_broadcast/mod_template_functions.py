from flask import jsonify, request 
import yaml
import os.path
import logging.config
import logging
import pandas as pd
import datetime 
import random
import time
import polars as pl
import io
from wacore.auth.mod_login_functions import phone_access_required

import requests
# Import custom packages
from ..pkg_extras.mod_common import ClsCommon
from waapi.celery_worker import async_broadcast_template_messages  # Here imported with new location ---- celery change ----
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from walogger.walogger import WaLogger

# Initialize logger with name that we want or any other
obj_log = WaLogger('pktmp')
lg = obj_log.get_logger()
# from datetime import datetime


# Calling class objects
obj_common = ClsCommon()


# with open(os.path.dirname(__file__) + '/../conf/logging.yaml', 'r') as f:
#     config = yaml.safe_load(f.read())
#     logging.config.dictConfig(config)
# LOG = logging.getLogger('templatesLog')    
xx= ""

class ClsBroadcast():
    """ Class called to send template broadcast """

    def __init__(self,client_number,broadcast_email_id,db_client_waba_settings,broadcast_name,template_name,excel_file,consen_header):
        """
        Initialize required object and variables:

        Parameters:
            client_number (str): Mobile Number of WhatsApp
            db_client_waba_settings (dict): All WABA Settings
            broadcast_name (str): Name of the current broadcast
            template_name (str): Name of the template for bulk broadcasting
            excel_file (binary): Excel binary file for broadcasting
        Local Variable:
            broadcast_id (int): Unique generated ID for each broadcast
            client_db_name(str): Fetch and generate DB name for individual client
            ew_db(DB object): Initialize DB
            client_db(DB Object):  Initialize DB
            contact_collection_name (collection Object name): Create collection name

        Returns:
            All above parameteres and variables
        """

        self.client_number = client_number
        self.broadcast_email_id = broadcast_email_id
        self.db_client_waba_settings = db_client_waba_settings
        self.broadcast_name = broadcast_name
        self.excel_file = excel_file
        self.template_name = template_name
        self.consen_header = consen_header
        self.broadcast_id = int(datetime.datetime.now().timestamp())-(random.randint(100, 999))+(random.randint(1000, 9999))
        self.client_db_name = db_client_waba_settings["response"]["ew_id"].lower() + "_" + db_client_waba_settings["response"]["waba_id"]
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        self.cl_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)
        self.contact_collection_name = "contacts_" + self.client_number
        self.db_template_mapping = {}
        self.temp_file_name = str(db_client_waba_settings["response"]["ew_id"].lower())+ "_" + str(template_name) + "_" +str(time.time()).replace(".","_")
        self.template_map =self.cl_db.template_mapping.find_one({"template_name":self.template_name},{"category":1,"language":1,"created_by":1})
        self.category = self.template_map["category"]
        # Insert Broadcast Document for Broadcating started and logs purpose
        self.cl_db.broadcast_details.insert_one(
                                                {"ew_id": str(db_client_waba_settings["response"]["ew_id"]), 
                                                "client_number": self.client_number, 
                                                "broadcast_id": self.broadcast_id, 
                                                "broadcast_name": self.broadcast_name, 
                                                "template_name": self.template_name, 
                                                "status": "ACCEPTED", 
                                                "recipient_total_count": 0, 
                                                "recipient_sent_count": 0, 
                                                "recipient_delivered_count": 0, 
                                                "recipient_read_count": 0, 
                                                "recipient_failed_count": 0, 
                                                "timestamp": int(datetime.datetime.now().timestamp() + (5 * 3600) + (30 * 60)), 
                                                "finished_timestamp": int(datetime.datetime.now().timestamp() + (5 * 3600) + (30 * 60)),
                                                "success_stats":[], 
                                                "failed_stats":[], 
                                                "sending_failed_stats":[],
                                                "upload_excel_url" : "", 
                                                "created_by": self.broadcast_email_id,
                                                "broadcast_logs":[],
                                                "category":self.category
                                                })
        log_data = "New broadcast started: " + "client_number: " + str(self.client_number) + ", broadcast_id: " + str(self.broadcast_id) + ", broadcast_name: " + str(self.broadcast_name) + ",template_name: " + str(self.template_name)
        lg.info(log_data)

    @phone_access_required
    def func_init_excel_broadcast(self,email_id,client_id,lg,consen_header):
        """
        Receive Excel file from POST and process as list
        
        Parameters: None
        
        Local Variables: None
        
        Returns:
            If excel format is correct, return job accepted
        """
        
        # ---------- convert and Verify Excel Start  ---------- 
        try:
            
            consen_header = self.consen_header
            lg.info(f"consen_header inside init_excel_broadcast function  is{consen_header} ")
            t1=  datetime.datetime.utcnow()
            file_name = "files/"+ str(self.temp_file_name)+".xlsx"
            # with open(file_name, "wb") as f:
            #     f.write(self.excel_file)
            file_obj = io.BytesIO(self.excel_file.read())

            df = pl.read_excel(file_obj)
            lg.info(f"head of df is {df.head()}")
            media_option = self.cl_db.template_mapping.find_one({"template_name": self.template_name}, {"_id": 0,"media_option":1})
            lg.info(f"value of media_id after data query is {media_option}")

            media_option_doc = self.cl_db.template_mapping.find_one(
    {"template_name": self.template_name},
    {"_id": 0, "media_option": 1})
            lg.info(f"Fetched media_option_doc: {media_option_doc} (type: {type(media_option_doc)})")
            if isinstance(media_option_doc, dict):
                media_option = media_option_doc.get("media_option", "on")
            else:
                lg.warning("Unexpected type for media_option_doc. Defaulting to 'on'.")
                media_option = "on"
            
            lg.info(f"media_option value is {media_option}")


            if str(media_option.lower()) == "off":
                    
                    media_id = self.cl_db.media_headr_handler.find_one({"template_name": self.template_name}, {"_id": 0,"media_id":1})
                    lg.info(f"media_id we want to add to df is {media_id}")
                    if media_id is not None:
                        media_id = media_id['media_id']
                        df = df.with_columns(pl.lit(int(media_id)).cast(pl.Int64).alias("MEDIA_ID"))
                    else:
                        lg.info(f"inside media_id is not present and not adding column name")
                        pass

            
            excel_column_names = list(df.head(0).columns)
            lg.info("ew_id=" + str(client_id) + " | " + "Excel column names : " + str(excel_column_names))
            #saving excel file over disk
            df.write_excel(file_name)
            df = {col: df[col].to_list() for col in df.columns}
            """
            t2 = datetime.datetime.utcnow()
            t3 = t2- t1
            lg.info(f"Time difference for 113 to 127 lines is {t3}")
            t4 =  datetime.datetime.utcnow()

            df_excel_file = pd.read_excel("files/"+ str(self.temp_file_name)+".xlsx")
            # ------ clean excel file end ------

            # df = df_excel_file.to_dict()
            df = read_file.to_dict()
            # lg.info(f"value of df after converting to dict is {df}")
            excel_column_names = list(df_excel_file.head(0).columns)
            lg.info("ew_id=" + str(client_id) + " | " + "Excel column names : " + str(excel_column_names))
            t5 =  datetime.datetime.utcnow()
            t6 = t5-t4
            lg.info(f"Time difference for 129  to 139 lines is {t6}")
            lg.info(f"Time before calling the add_broadcast_log function is {datetime.datetime.utcnow()}")
            """

            obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Excel upload sucessful.")
            lg.info(f"Time After calling the add_broadcast_log function is {datetime.datetime.utcnow()}")
            t7 =  datetime.datetime.utcnow()

        except Exception as e:
            obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Excel upload failed.")
            lg.critical("ew_id=" + str(client_id) + " | " + "Excel upload failed. : " + str(e))    
            return jsonify({"id": "1173", "message": "Excel upload failed.", "description": "Excel error. Please check excel file again", "data": "", "success": False , "Broadcast_id": self.broadcast_id})
        # ---------- convert and Verify Excel End  ---------- 

        # ---------- Extract header columns from DB start  ---------- 
        try:
            db_user_roles = self.cl_db.user_roles.find_one({"email_id": email_id}, {"_id":0})
            if int(self.client_number) not in db_user_roles["accessible_phones"]: #CHANGE
                return jsonify({"id": "1174", "message": "User role is not defined", "description": "You don't have permission to broadcast from this number", "data": "", "success": False , "Broadcast_id": self.broadcast_id})
            t8 =  datetime.datetime.utcnow()
            lg.info(f"Time difference for 143  to 156 lines is {t8-t7}")
            db_template_mapping = self.cl_db.template_mapping.find_one({"template_name": self.template_name}, {"_id": 0})
            lg.info("ew_id=" + str(client_id) + " | " + "db_template_mapping : " + str(db_template_mapping))
            t9 = datetime.datetime.utcnow()
            if db_template_mapping != None:
                column_names_db = []
                column_names_db.append("MOBILE_NUMBER")
                column_names_db.append("TEMPLATE_ID")
                column_names_db.append("TEMPLATE_NAME")

                if db_template_mapping["header"]["header_text_column_name"] != "":
                    column_names_db.append(db_template_mapping["header"]["header_text_column_name"])
                
                if db_template_mapping["header"]["attachment_column_name"] != "":
                    column_names_db.append(db_template_mapping["header"]["attachment_column_name"])

                t10 = datetime.datetime.utcnow()
                lg.info(f"Time difference for 160  to 173 lines is {t10-t9}")
                if db_template_mapping["body"]["body_column_names"] != []:
                    body_column_names = db_template_mapping["body"]["body_column_names"]
                    for body_data in body_column_names:
                        column_names_db.append(body_data)

                t11 = datetime.datetime.utcnow()
                if "button" in db_template_mapping.keys():
                    if "button1" in db_template_mapping["button"]:
                        if "dynamic_url_column_name" in db_template_mapping["button"]["button1"]:
                            if db_template_mapping["button"]["button1"]["dynamic_url_column_name"] != "":
                                column_names_db.append(db_template_mapping["button"]["button1"]["dynamic_url_column_name"])
                        
                    if  "button2" in db_template_mapping["button"]:
                        if "dynamic_url_column_name" in db_template_mapping["button"]["button2"]:
                            if db_template_mapping["button"]["button2"]["dynamic_url_column_name"] != "":
                                column_names_db.append(db_template_mapping["button"]["button2"]["dynamic_url_column_name"])
                self.db_template_mapping = db_template_mapping
                obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Template found and columns processed.")
                t12 = datetime.datetime.utcnow()
                lg.info(f"Time difference between 180 to 193 lines {t12-t11}")
            else:
                obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Template does not exist or columns process failed")
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - template_mapping, Template does not exists in DB: None")    
                return jsonify({"id": "1175", "message": "Template does not exists.", "description": "Incorrect template name or Template not present", "data": "", "success": False , "Broadcast_id": self.broadcast_id})
            t13 = datetime.datetime.utcnow()
        except Exception as e:
            obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Template column processing failed.")
            lg.critical("ew_id=" + str(client_id) + " | " + "Template column processing failed : " + str(e))    
            return jsonify({"id": "1176", "message": "Template column processing failed", "description": "Error in user key or data query error", "data": "", "success": False , "Broadcast_id": self.broadcast_id})
        # ---------- Extract header columns from DB End  ---------- 


        # ---------- Compare Excel Columns Headers and DB Columns Start  ---------- 
        excel_column_names = sorted(excel_column_names)
        lst_columns_db = sorted(column_names_db)
        t14 = datetime.datetime.utcnow()
        lg.info(f"Time differnce between 199 line to 210 lines is {t14-t13}")
        t15 = datetime.datetime.utcnow()
        if excel_column_names == lst_columns_db:
            # Submit Excel for celary job
            # Without client_waba_settings arguments i.e db_client_waba_settings
            obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Template column match completed.")
            kwargs_params = {"ew_id":self.db_client_waba_settings["response"]["ew_id"], "client_number":self.client_number, "template_name":self.template_name, "broadcast_id":self.broadcast_id, "broadcast_name":self.broadcast_name, "client_db_name":self.client_db_name, "db_template_mapping":self.db_template_mapping,"language":db_template_mapping["language"]}
            lg.info("ew_id=" + str(client_id) + " | " + "before adding celery - kwargs_params  : " + str(kwargs_params))
            # t123 = datetime.datetime.utcnow()
            lg.info(f"Time before calling the async broadcast_template_message function is {datetime.datetime.utcnow()}")
            # celery_response = async_broadcast_template_messages.apply_async(args=[column_names_db,df,email_id],kwargs=kwargs_params)
            celery_response =  async_broadcast_template_messages.apply_async(args=[column_names_db, df, email_id, consen_header, media_option],kwargs=kwargs_params, queue="high_priority")
            lg.info(f"Time After  calling the async broadcast_template_message function is {datetime.datetime.utcnow()}")
            # t456 = datetime.datetime.utcnow()
            # lg.info(f"Timedifference for async broadcast_template_message functions  {t123-t456}")
            obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Submited successfully, sending will start in 1-5 Minutes")
            t16 = datetime.datetime.utcnow()
            lg.info(f"Timedifference between 212 line to 222 line is {t16-t15}")
            return jsonify({"id": "1177","message": "Submited successfully, Sending will start in 1-5 Minutes", "description": "", "data": "", "success": True , "Broadcast_id": self.broadcast_id})
        else:
            obj_common.func_add_broadcast_log(self.cl_db,self.broadcast_id,"Template and excel column match failed, sending will not start")
            lg.critical("ew_id=" + str(client_id) + " | " + "Template and excel column match failed, sending will not start")    
            return jsonify({"id": "1178", "message": "Template and excel column match failed, sending will not start", "description": "Excel error. Columns not matching", "data": "", "success": False,"Broadcast_id": self.broadcast_id})

        # ---------- Compare Excel Columns Headers and DB Columns End  ---------- 

    def __del__(self):
        self.ew_db.client.close()
        self.cl_db.client.close()
        
###################################End File ##################################################


class ClsBroadcastoperations():
    """ Class called to perform broacast related actions """

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


    @phone_access_required
    def func_get_broadcast_list_function(self,str_client_id,str_email_id,page_size,page_num,int_from_timestamp,int_to_timestamp,lg,search_pattern):
        """ Method called to get list of broadcst details """
        try:

            #newly added logic to skip email_id check if email_id is user_admin
            db_user_role = self.cl_db.user_roles.find_one({"email_id": str_email_id}, {"_id": 0})
            if db_user_role == None:
                user_role = ""
            else: 
               user_role = db_user_role["role"]   
        
            if user_role == "DMP Admin":
                # lg.info("inside dmpadmin condition")
                skips = page_size * (page_num - 1)
                if search_pattern == "":
                    broadcast_details =(list(self.cl_db.broadcast_details.find({"ew_id": str_client_id, "timestamp": {"$gte": int_from_timestamp, "$lt": int_to_timestamp}}, {"broadcast_id": 1,"broadcast_name": 1,"client_number": 1,"created_by": 1,"recipient_total_count":1,"status":1,"broadcast_logs":1,"timestamp":1,"finished_timestamp":1,"_id": 0})))
                    db_broadcast_details = (broadcast_details[::-1][(skips):(skips+page_size)])
                else:
                    broadcast_details =(list(self.cl_db.broadcast_details.find({"ew_id": str_client_id, "timestamp": {"$gte": int_from_timestamp, "$lt": int_to_timestamp},"broadcast_name": {"$regex": search_pattern, "$options": "i"}}, {"broadcast_id": 1,"broadcast_name": 1,"client_number": 1,"created_by": 1,"recipient_total_count":1,"status":1,"broadcast_logs":1,"timestamp":1,"finished_timestamp":1,"_id": 0})))
                    db_broadcast_details = (broadcast_details[::-1][(skips):(skips+page_size)])

            else:
                    # lg.info("inside else block of dmp_admin check")
                    #----------------Another Approach (using mongodb skip)------------
                    # broadcasts_count = len(list(self.cl_db.broadcast_details.find({"ew_id": str_client_id, "timestamp": {"$gte": int_from_timestamp, "$lt": int_to_timestamp}, "created_by": str_email_id}, {"_id": 0})))
                    # skips = page_size * (page_num - 1)
                    # db_broadcast_details = list(self.cl_db.broadcast_details.find({"ew_id": str_client_id, "timestamp": {"$gte": int_from_timestamp, "$lt": int_to_timestamp}, "created_by": str_email_id}, {"_id": 0}).skip(skips).limit(page_size))
                    #---------------------------------------
                    skips = page_size * (page_num - 1)
                    ##broadcast_details =(list(self.cl_db.broadcast_details.find({"ew_id": str_client_id, "timestamp": {"$gte": int_from_timestamp, "$lt": int_to_timestamp}, "created_by": str_email_id}, {"_id": 0})))
                    ##db_broadcast_details = broadcast_details[(skips):(skips+page_size)]
                    broadcast_details =(list(self.cl_db.broadcast_details.find({"ew_id": str_client_id, "timestamp": {"$gte": int_from_timestamp, "$lt": int_to_timestamp}, "created_by": str_email_id}, {"broadcast_id": 1,"broadcast_name": 1,"client_number": 1,"created_by": 1,"recipient_total_count":1,"status":1,"broadcast_logs":1,"timestamp":1,"finished_timestamp":1,"_id": 0})))
                    db_broadcast_details = (broadcast_details[::-1][(skips):(skips+page_size)])
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "Error in excel file : " + str(e))    
            return jsonify({"id": "1184", "message": "Data Query Error", "description": "", "data": "", "success": False})
    
        return jsonify({"id": "1185", "message": "Data fetched successfully", "description": "", "data": {"broadcast_details":db_broadcast_details,"count":len(broadcast_details)}, "success": True})


    @phone_access_required
    def func_delete_broadcast_function(self,str_client_id,str_broadcast_id,lg):
        """ Method called to delete broadcast details """
        try:
            db_broadcast_details = self.cl_db.broadcast_details.delete_one({"ew_id": str_client_id, "broadcast_id": str_broadcast_id})
        except Exception as e:
            lg.critical("ew_id=" + str(str_client_id) + " | " + "Error in excel file : " + str(e))    
            return jsonify({"id": "1194", "message": "Data Query Error", "description": "", "data": "", "success": False})
    
        return jsonify({"id": "1195", "message": "Broadcast deleted successfully", "description": "", "data": "", "success": True})
        
    # @phone_access_required
    # def get_media_id(self, str_client_id, media_file_path, lg, phonenumberid):
    #     import os
    #     import mimetypes
    #     import requests
    #     from datetime import datetime
    #     from io import BytesIO

    #     if not os.path.exists(media_file_path):
    #         lg.error(f"File not found: {media_file_path}")
    #         return {"error": "File not found"}, 404

    #     try:
    #         file_stats = os.stat(media_file_path)
    #         if file_stats.st_size == 0:
    #             lg.error(f"Empty file detected: {media_file_path}")
    #             return {"error": "File is empty (0 bytes)"}, 400
                
    #         with open(media_file_path, 'rb') as f:
    #             file_content = f.read()
    #             if not file_content:
    #                 lg.error(f"Read 0 bytes from: {media_file_path}")
    #                 return {"error": "File read returned no data"}, 400
                    
    #     except Exception as e:
    #         lg.error(f"File access error: {str(e)}")
    #         return {"error": "File access failed"}, 500

    #     try:
    #         mime_type = mimetypes.guess_type(media_file_path)[0] or 'application/octet-stream'
    #         headers = {
    #             "Authorization": "Bearer YOUR_ACCESS_TOKEN"
    #         }
            
    #         files = {
    #             'file': (
    #                 os.path.basename(media_file_path),
    #                 BytesIO(file_content), 
    #                 mime_type
    #             )
    #         }
            
    #         response = requests.post(
    #             f"https://graph.facebook.com/v16.0/{phonenumberid}/media",
    #             headers=headers,
    #             files=files,
    #             data={'messaging_product': 'whatsapp'}
    #         )
            
    #         response.raise_for_status()
    #         return response.json().get('id')
            
    #     except requests.exceptions.RequestException as e:
    #         lg.error(f"Upload failed: {str(e)}")
    #         return {"error": "Media upload failed"}, 500
    #     finally:
    #         try:
    #             if os.path.exists(media_file_path):
    #                 os.remove(media_file_path)
    #         except Exception as e:
    #             lg.error(f"File cleanup failed: {str(e)}")
    @phone_access_required
    def get_media_id(self, str_client_id, media_file, lg, phonenumberid):
        import mimetypes

        WHATSAPP_API_URL = f"https://graph.facebook.com/v23.0/{phonenumberid}/media"
        lg.info(f"WhatsApp API URL for getting media_id is {WHATSAPP_API_URL}")

        # Determine MIME type dynamically
        mime_type, _ = mimetypes.guess_type(media_file)
        if mime_type is None:
            lg.error(f"Unable to determine MIME type for file: {media_file}")
            return jsonify({"error": "Unsupported file type"}), 400

        headers = {
            "Authorization": f"Bearer EAAjcKG57dJcBO3zuOL9JkZCU90M4T0a6wALuCHqz61P1E8uIiJxZC6h91jwiJUa3cBay9EX8trp4WZBgCmMcZCL9jtx6OetkCyzRQMEZBtnWlXB85ZBaWYAW0dHvwU3ZCMNZAZBPeeNkKz6ZBqEC0z8dExZAckFirt56aXI2ZAN7XEIXrUl2ZCn2XWedBVycJ8euugSNs"
        }
        
        files = {"file": (media_file, open(media_file, "rb"), mime_type)}
        data = {
            "type": mime_type,
            "messaging_product": "whatsapp"
        }

        lg.info(f"Media ID request is {data} and file {files}")

        try:
            response = requests.post(WHATSAPP_API_URL, headers=headers, files=files, data=data)
            response_data = response.json()
            lg.info(f"WhatsApp API response for getting media_id is {response.text}")
        except Exception as e:
            lg.error(f"Error while uploading media: {str(e)}")
            return jsonify({"error": "Failed to upload media"}), 500
        finally:
            os.remove(media_file)

        return jsonify(response_data), response.status_code