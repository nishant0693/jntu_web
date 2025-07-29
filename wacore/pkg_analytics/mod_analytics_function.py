from flask import jsonify
import datetime
import requests
import json

from pymongo import UpdateOne 
import time
#Import custom packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from wacore.pkg_extras.mod_common import ClsCommon
from wacore.auth.mod_login_functions import phone_access_required
from walogger.walogger import WaLogger
# from waapi.celery_worker3 import async_updateanalytics_pending

obj_log = WaLogger('pktmp')
lg = obj_log.get_logger()


class ClsAnalytics():
    def __init__(self,db_client_waba_settings):
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
    def func_list_of_numbers(self,client_id,accessible_phones,lg):
        try:
            db_client_business_info = self.ew_db.client_business_info.find_one({"ew_id": client_id}, {"_id":0})
        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - client_business_info : " + str(e))    
            return jsonify({"id": "1204", "message": "Data Query Error", "description": "", "data": "", "success": False})

        lst_phone_numbers = []
        lst_phone_data = list(map(str, accessible_phones))
        for number_data in db_client_business_info["wa_phone_numbers"]:
            cc_client_number = number_data["country_code"][1:] + number_data["wa_number"]
            if cc_client_number in lst_phone_data:
                lst_phone_numbers.append(cc_client_number)

        return jsonify({"id": "1205", "message": "Data fetched successfully", "description": "", "data": {"client_numbers": lst_phone_numbers}, "success": True})


class ClsDmpServiceAnalytics():
    """class for dmp service analytics(general messages and send to wa)"""
    
    def __init__(self,client_number):
        """ Class for calling DMP analytics fuctions
    
        Initialize required object and variables:

        Parameters:
            client_number (str): Mobile Number of WhatsApp
            str_template_name (str): name of the template sent 
            db_client_waba_settings (dict): All WABA Settings
            
            str_recipient_number : 
        Local Variable:
            client_db_name(str): Fetch and generate DB name for individual client
            ew_db(DB object): Initialize DB
            client_db(DB Object):  Initialize DB
            db_dmps_send_to_wa_analytics (list): list of documents in dmps_send_to_wa_analytics collection
            db_general_message_details (list): list of documents in dmps_send_to_wa_analytics collection
            text_messages_count : count of text messages sent in given time interval   
            image_messages_count : count of images messages sent in given time interval
            audio_messages_count : count of audio messages sent in given time interval   
            video_messages_count : count of video messages sent in given time interval
            document_messages_count : count of documnent messages sent in given time interval   
            location_messages_count : count of location messages sent in given time interval 
            contact_messages_count : count of contact messages sent in given time interval   
            sticker_messages_count: count of sticker messages sent in given time interval    
            sent_messages_count : count of messages sent in given time interval 
            delivered_messages_count : count of  messages delivered in given time interval
            read_messages_count : count of  messages read in given time interval
            failed_messages_count : count of failed messages  in given time interval
            spm_messages_count : count of spm messages sent in given time interval
            mpm_messages_count : count of mpm messages sent in given time interval
            sent_count : count of template messages sent in given time interval 
            delivered_count : count of template messages sent in given time interval 
            read_count : count of template messages sent in given time interval 
            failed_count : count of template messages sent in given time interval 
        Returns:
            All above parameteres and variables
      
        """

        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        obj_common = ClsCommon()
        self.client_number = str(client_number)
        db_client_waba_settings = obj_common.get_waba_settings_by_cc_client_number(self.client_number)
        if "error" in db_client_waba_settings:
            return jsonify({"error": "Invalid client number. Please enter appropriate client number"})    
        self.client_db_name = str(db_client_waba_settings["response"]["ew_id"]).lower() + "_" + db_client_waba_settings["response"]["waba_id"]
        self.client_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)

    #-------------------------------------Send to WhatsApp Analytics start-------------------------------------------------- 


    def func_send_to_wa_analytics(self,str_recipient_number,str_message_id,str_template_name,flag,bot_id,lg,message_body,resp):
        """Call this function for recording data for analytics when template is sent from dmp"""
        current_timestamp = int(datetime.datetime.now().timestamp())
        dt_object_cur = datetime.datetime.utcfromtimestamp(current_timestamp)
        date_cur = dt_object_cur.strftime("%d")
        month_cur = dt_object_cur.strftime("%m")
        year_cur = dt_object_cur.strftime("%Y")
        datemonth_cur = str(date_cur+month_cur+year_cur)
        lg.info("entered inside func_send_to_wa_analytics")
        if flag == "dmps":
            try:
                message_body = json.loads(message_body)
                db_dmps_send_to_wa_analytics = list(self.client_db.dmps_send_to_wa_analytics.find({"client_number": self.client_number, "template_name":str_template_name}, {"_id":0}))
                if db_dmps_send_to_wa_analytics == []:
                    self.client_db.dmps_send_to_wa_analytics.insert_one({"client_number": self.client_number, "template_name": str_template_name, "recipient_sent_count": 1, "recipient_delivered_count": 0, "recipient_read_count": 0, "timestamp": current_timestamp, "success_stats": [{"message_id": str_message_id, "conversation_id": "", "conversation_type": "", "billable": True, "recipient_number": str_recipient_number,"timestamp": current_timestamp, "sent": False, "delivered": False, "read": False,"button_response":"NA", "dynamic_variables":message_body['template']['components']}],"failed_stats":[]})
                else:
                    int_sent_messages_count = db_dmps_send_to_wa_analytics[-1]["recipient_sent_count"]
                    if int_sent_messages_count == 100:
                        self.client_db.dmps_send_to_wa_analytics.insert_one({"client_number": self.client_number, "template_name": str_template_name, "recipient_sent_count": 1, "recipient_delivered_count": 0, "recipient_read_count": 0, "timestamp": current_timestamp, "success_stats": [{"message_id": str_message_id, "conversation_id": "", "conversation_type": "", "billable": True, "recipient_number": str_recipient_number,"timestamp": current_timestamp, "sent": False, "delivered": False, "read": False,"button_response":"NA", "dynamic_variables":message_body['template']['components']}],"failed_stats":[]})
                    else:
                        self.client_db.dmps_send_to_wa_analytics.update_one({"client_number": self.client_number,"template_name": str_template_name,"recipient_sent_count": {"$lt":100}},{"$push": {"success_stats": {"message_id": str_message_id, "conversation_id": "", "conversation_type": "", "billable": True, "recipient_number": str_recipient_number,"timestamp": current_timestamp, "sent": False, "delivered": False, "read": False,"button_response":"NA", "dynamic_variables":message_body['template']['components']}},"$inc": {"recipient_sent_count": 1}})
            except Exception as e:
                lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - dmp_send_to_wa_analytics: " + str(e))
                return jsonify({"id": "5005", "response": "Data Query Error", "success": False})
        else:
            try:
                message_body = json.loads(message_body)
                self.client_db.client_sent_to_wa_analytics_info.insert_one({"client_number": self.client_number, "template_name": str_template_name,"recipient_number": str_recipient_number,"sent": True,"message_response_id":str_message_id,"whatsappresp":resp,"timestamp":current_timestamp,"date_month":datemonth_cur})
                db_client_sent_to_wa_analytics = list(self.client_db.client_sent_to_wa_analytics.find({"client_number": self.client_number, "template_name":str_template_name,"date_month":datemonth_cur}))
                lg.info(f"current day_month is {datemonth_cur}")
                if db_client_sent_to_wa_analytics==[]:
                     lg.info("inside if of client_api valid")
                     self.client_db.client_sent_to_wa_analytics.insert_one({"client_number": self.client_number, "template_name": str_template_name,"recipient_total_count":1, "recipient_sent_count": 1,"recipient_delivered_count": 0, "recipient_read_count": 0,"recipient_failed_count":0, "timestamp": current_timestamp,"date_month":datemonth_cur})
                else:
                   lg.info("inside else of client_api valid")
                   self.client_db.client_sent_to_wa_analytics.find_one_and_update({"template_name": str_template_name,"date_month":datemonth_cur},{"$inc": {"recipient_sent_count":1,"recipient_total_count":1}})
                # db_client_sent_to_wa_analytics_info = list(self.client_db.client_sent_to_wa_analytics_info.find({"client_number": self.client_number, "template_name":str_template_name}, {"_id":0}))
                    # db_broadcast_ = self.client_db.client_sent_to_wa_analytics_info.find_one_and_update({"client_number": self.client_number, "template_name":str_template_name},{"$set":{"delivered":True,}})            

                # db_client_sent_to_wa_analytics = list(self.client_db.client_sent_to_wa_analytics.find({"client_number": self.client_number, "template_name":str_template_name}, {"_id":0}))
                # if db_client_sent_to_wa_analytics == []:
                #     self.client_db.client_sent_to_wa_analytics.insert_one({"client_number": self.client_number, "template_name": str_template_name, "recipient_sent_count": 1, "recipient_delivered_count": 0, "recipient_read_count": 0, "timestamp": current_timestamp, "success_stats": [{"message_id": str_message_id, "conversation_id": "", "conversation_type": "", "billable": True, "recipient_number": str_recipient_number,"timestamp": current_timestamp, "sent": False, "delivered": False, "read": False,"button_response":"NA", "dynamic_variables":message_body['template']['components']}],"failed_stats":[]})
                # else:
                #     int_sent_messages_count = db_client_sent_to_wa_analytics[-1]["recipient_sent_count"]
                #     if int_sent_messages_count == 100:
                #         self.client_db.client_sent_to_wa_analytics.insert_one({"client_number": self.client_number, "template_name": str_template_name, "recipient_sent_count": 1, "recipient_delivered_count": 0, "recipient_read_count": 0, "timestamp": current_timestamp, "success_stats": [{"message_id": str_message_id, "conversation_id": "", "conversation_type": "", "billable": True, "recipient_number": str_recipient_number,"timestamp": current_timestamp, "sent": False, "delivered": False, "read": False,"button_response":"NA", "dynamic_variables":message_body['template']['components']}],"failed_stats":[]})
                #     else:
                #         self.client_db.client_sent_to_wa_analytics.update_one({"client_number": self.client_number,"template_name": str_template_name,"recipient_sent_count": {"$lt":100}},{"$push": {"success_stats": {"message_id": str_message_id, "conversation_id": "", "conversation_type": "", "billable": True, "recipient_number": str_recipient_number,"timestamp": current_timestamp, "sent": False, "delivered": False, "read": False,"button_response":"NA", "dynamic_variables":message_body['template']['components']}}})
            except Exception as e:
                lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - dmp_send_to_wa_analytics : " + str(e))
                return jsonify({"id": "5005", "response": "Data Query Error", "success": False})
        

    def func_send_to_wa_invalid(self,contact_status,str_recipient_number,str_client_id,str_template_name,flag,bot_id,lg,resp):  
        """Call this function when invalid contact"""
        current_timestamp = int(datetime.datetime.now().timestamp())
        dt_object_cur = datetime.datetime.utcfromtimestamp(current_timestamp)
        date_cur = dt_object_cur.strftime("%d")
        month_cur = dt_object_cur.strftime("%m")
        year_cur = dt_object_cur.strftime("%Y")
        datemonth_cur = str(date_cur+month_cur+year_cur)
        lg.info(f"Response is {resp}")
        error_data = resp['error']
        # contact_status = "invalid"
        if flag == "client":
            try: 
                self.client_db.client_sent_to_wa_analytics_info.insert_one({"client_number": self.client_number, "template_name": str_template_name,"recipient_number": str_recipient_number,"sent": "NA","message_response_id":"None","whatsappresp":resp,"timestamp": current_timestamp,"date_month":datemonth_cur,"reason":"Invalid Number"})
                db_client_sent_to_wa_analytics = list(self.client_db.client_sent_to_wa_analytics.find({"client_number": self.client_number, "template_name":str_template_name,"date_month":datemonth_cur}))
                if db_client_sent_to_wa_analytics==[]:
                     self.client_db.client_sent_to_wa_analytics.insert_one({"client_number": self.client_number, "template_name": str_template_name,"recipient_total_count":1, "recipient_sent_count": 0,"recipient_delivered_count": 0, "recipient_read_count": 0,"recipient_failed_count":1, "timestamp": current_timestamp,"date_month":datemonth_cur})
                else:
                  self.client_db.client_sent_to_wa_analytics.find_one_and_update({"template_name": str_template_name,"date_month":datemonth_cur},{"$inc": {"recipient_failed_count":1,"recipient_total_count":1}}) 
                # db_client_sent_to_wa_analytics = list(self.client_db.client_sent_to_wa_analytics.find({"client_number": self.client_number, "template_name":str_template_name}, {"_id":0}))
                # if db_client_sent_to_wa_analytics ==[]:
                #     self.client_db.client_sent_to_wa_analytics.insert_one({"client_number": self.client_number, "template_name": str_template_name, "recipient_sent_count": 0, "recipient_delivered_count": 0, "recipient_read_count": 0, "timestamp": int(datetime.datetime.now().timestamp()), "success_stats": [],"failed_stats":[{"recipient_number": str_recipient_number,"error":contact_status,"error_details": error_data,"timestamp": int(datetime.datetime.now().timestamp())}]})
                #     # ew_db.dmps_send_to_wa_analytics.insert_one({"client_number": str_client_number, "template_name": str_template_name, "recipient_sent_count": 0, "recipient_delivered_count": 0, "recipient_read_count": 0, "timestamp": int(datetime.now().timestamp()), "success_stats": [],"failed_stats":[{"recipient_number": str_recipient_number,"error":error,"timestamp": int(datetime.now().timestamp())}]})   
                # else:   
                #     db_doc_client = self.client_db.client_sent_to_wa_analytics.find_one_and_update({"client_number":self.client_number,"template_name":str_template_name,"recipient_sent_count": {"$lt":100}},{"$push":{"failed_stats":{"recipient_number": str_recipient_number,"error":contact_status,"error_details": error_data,"timestamp": int(datetime.datetime.now().timestamp())}}})
                #     if db_doc_client == None:
                #         self.client_db.client_sent_to_wa_analytics.insert_one({"client_number": self.client_number, "template_name": str_template_name, "recipient_sent_count": 0, "recipient_delivered_count": 0, "recipient_read_count": 0, "timestamp": int(datetime.datetime.now().timestamp()), "success_stats": [],"failed_stats":[{"recipient_number": str_recipient_number,"error":contact_status,"error_details": error_data,"timestamp": int(datetime.datetime.now().timestamp())}]})
            except Exception as e:
                lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - client_send_to_wa_analytics : " + str(e))
                return jsonify({"id": "5005", "response": "Data Query Error", "success": False})
        else:
            try:  
                db_dmps_send_to_wa_analytics = list(self.client_db.dmps_send_to_wa_analytics.find({"client_number": self.client_number, "template_name":str_template_name}, {"_id":0}))
                if db_dmps_send_to_wa_analytics ==[]:
                    self.client_db.dmps_send_to_wa_analytics.insert_one({"client_number": self.client_number, "template_name": str_template_name, "recipient_sent_count": 0, "recipient_delivered_count": 0, "recipient_read_count": 0, "timestamp": int(datetime.datetime.now().timestamp()), "success_stats": [],"failed_stats":[{"recipient_number": str_recipient_number,"error":contact_status,"error_details": error_data,"timestamp": int(datetime.datetime.now().timestamp())}]})
                    # ew_db.dmps_send_to_wa_analytics.insert_one({"client_number": str_client_number, "template_name": str_template_name, "recipient_sent_count": 0, "recipient_delivered_count": 0, "recipient_read_count": 0, "timestamp": int(datetime.now().timestamp()), "success_stats": [],"failed_stats":[{"recipient_number": str_recipient_number,"error":error,"timestamp": int(datetime.now().timestamp())}]})   
                else:                   
                    db_doc_dmps = self.client_db.dmps_send_to_wa_analytics.find_one_and_update({"client_number":self.client_number,"template_name":str_template_name,"recipient_sent_count": {"$lt":100}},{"$push":{"failed_stats":{"recipient_number": str_recipient_number,"error":contact_status,"error_details": error_data,"timestamp": int(datetime.datetime.now().timestamp())}}})
                    if db_doc_dmps == None:
                        self.client_db.dmps_send_to_wa_analytics.insert_one({"client_number": self.client_number, "template_name": str_template_name, "recipient_sent_count": 0, "recipient_delivered_count": 0, "recipient_read_count": 0, "timestamp": int(datetime.datetime.now().timestamp()), "success_stats": [],"failed_stats":[{"recipient_number": str_recipient_number,"error":contact_status,"error_details": error_data,"timestamp": int(datetime.datetime.now().timestamp())}]})
            except Exception as e:
                lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - client_send_to_wa_analytics : " + str(e))
                return jsonify({"id": "5005", "response": "Data Query Error", "success": False})

        return jsonify({"response":"message sent failed"})


    def func_get_send_to_wa_analytics(self,str_template_name,int_from_timestamp,int_to_timestamp,bool_all,flag,lg):
            """Call this function to fetch analytics of templates sent from dmp"""           
            if bool_all == True:
                try:
                    if flag == "dmps":
                        db_list_counts = list(self.client_db.dmps_send_to_wa_analytics.aggregate([{"$match": {"client_number":self.client_number}},           
                                {"$addFields":{"sent_count": {"$filter": {"input" : "$success_stats", "as":"part","cond":{"$and":[{"$gt":["$$part.timestamp",int_from_timestamp]}, {"$lt":["$$part.timestamp",int_to_timestamp]},{"$eq":["$$part.sent",True]}]}}},                                         
                                                "delivered_count": {"$filter": {"input": "$success_stats","as": "part", "cond": {"$and":[{"$gt": [ "$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]},{"$eq": ["$$part.delivered", True]}]}}},                                                                                                                                                                       
                                                "read_count": {"$filter": {"input": "$success_stats","as": "part", "cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.read", True]}]}}},                                                                                                                                                                               
                                                "failed_count": {"$filter": {"input": "$failed_stats", "as": "part", "cond": {"$and": [{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}]}}} }}]))                   
                    else:
                        db_list_counts = list(self.client_db.client_sent_to_wa_analytics.find({"client_number":self.client_number, "timestamp": {"$gte": int_from_timestamp, "$lte": int_to_timestamp}})) 
                        lg.info(f"length of Analytics list is {len(db_list_counts)}")
                        # db_list_counts = list(self.client_db.client_sent_to_wa_analytics.aggregate([{"$match": {"client_number":self.client_number}},           
                        #         {"$addFields":{"sent_count": {"$filter": {"input" : "$success_stats", "as":"part","cond":{"$and":[{"$gt":["$$part.timestamp",int_from_timestamp]}, {"$lt":["$$part.timestamp",int_to_timestamp]},{"$eq":["$$part.sent",True]}]}}},                                         
                        #                         "delivered_count": {"$filter": {"input": "$success_stats","as": "part", "cond": {"$and":[{"$gt": [ "$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]},{"$eq": ["$$part.delivered", True]}]}}},                                                                                                                                                                       
                        #                         "read_count": {"$filter": {"input": "$success_stats","as": "part", "cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.read", True]}]}}},                                                                                                                                                                               
                        #                         "failed_count": {"$filter": {"input": "$failed_stats", "as": "part", "cond": {"$and": [{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}]}}} }}]))                   

                except Exception as e:
                    lg.critical("DB error - dmps_send_to_wa_analytics : " + str(e))
                    return jsonify({"id": "2002", "msg": "Data Query Error", "description": "", "data": "", "success": False})
                

                result = []
                template_list = []
                for document in db_list_counts:                
                    template_name = document["template_name"]
                    if template_name not in template_list:
                        template_list.append(template_name)
                        template_doc_list = list(filter(lambda x:x["template_name"] == template_name, db_list_counts))
                        lg.info(f"Template doc_list is {template_doc_list}")
                        sent_count = 0
                        delivered_count = 0
                        read_count = 0
                        failed_count = 0
                        for list_count in template_doc_list :           
                            if list_count["recipient_sent_count"] != []:
                                sent_count += list_count["recipient_sent_count"]
                            if list_count["recipient_delivered_count"] != []:
                                delivered_count += list_count["recipient_delivered_count"]
                            if list_count["recipient_read_count"] != []:
                                read_count += list_count["recipient_read_count"]
                            if list_count["recipient_failed_count"] != []:
                                failed_count += list_count["recipient_failed_count"]
                        res = {"template_name":template_name,"sent_messages_count":(sent_count),"delivered_messages_count":(delivered_count),"read_messages_count":(read_count),"failed_messages_count":(failed_count)}
                        res.update({"total_messages_count": (sent_count + failed_count)})
                        cost = 0.80 * (sent_count + failed_count)
                        res.update({"cost": cost})
                        #---------------------------------------------------
                        if res["total_messages_count"] !=0:
                            result.append(res)
                        #-------------------------------------------------
                    else:
                        pass

                # ---------------------------------------------------------
                total = {"total_sent_count":sum(item["sent_messages_count"] for item in result),"total_delivered_count":sum(item["delivered_messages_count"] for item in result),"total_read_count":sum(item["read_messages_count"] for item in result),"total_failed_count":sum(item["failed_messages_count"] for item in result)}            
                total.update({"total_count" : (total["total_sent_count"] + total["total_failed_count"])})
                total.update({"total_cost":0.80*total["total_count"]})
                # -------------------------------------------------------
                return {"id": "2003", "message": "Data fetched successfully", "description": "", "data":result,"total_count":total, "success":True}

            else:
                try:
                    if flag == "dmps":
                        db_list_counts = list(self.client_db.dmps_send_to_wa_analytics.aggregate([{"$match": {"client_number": self.client_number,"template_name":str_template_name}},           
                                    {"$addFields":{"sent_count": {"$filter": {"input" : "$success_stats", "as":"part","cond":{"$and":[{"$gt":["$$part.timestamp",int_from_timestamp]}, {"$lt":["$$part.timestamp",int_to_timestamp]},{"$eq":["$$part.sent",True]}]}}},                                         
                                                    "delivered_count": {"$filter": {"input": "$success_stats","as": "part", "cond": {"$and":[{"$gt": [ "$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]},{"$eq": ["$$part.delivered", True]}]}}},                                                                                                                                                                       
                                                    "read_count": {"$filter": {"input": "$success_stats","as": "part", "cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.read", True]}]}}},                                                                                                                                                                               
                                                    "failed_count": {"$filter": {"input": "$failed_stats", "as": "part", "cond": {"$and": [{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}]}}} }}]))       
                    else:
                        db_list_counts =  list(self.client_db.client_sent_to_wa_analytics.find({"client_number":self.client_number,"template_name":str_template_name ,"timestamp": {"$gte": int_from_timestamp, "$lte": int_to_timestamp}})) 
                        lg.info(f"length analyics list for individual template is {len(db_list_counts)}")
                        # db_list_counts = list(self.client_db.client_sent_to_wa_analytics.aggregate([{"$match": {"client_number": self.client_number,"template_name":str_template_name}},           
                        #             {"$addFields":{"sent_count": {"$filter": {"input" : "$success_stats", "as":"part","cond":{"$and":[{"$gt":["$$part.timestamp",int_from_timestamp]}, {"$lt":["$$part.timestamp",int_to_timestamp]},{"$eq":["$$part.sent",True]}]}}},                                         
                        #                             "delivered_count": {"$filter": {"input": "$success_stats","as": "part", "cond": {"$and":[{"$gt": [ "$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]},{"$eq": ["$$part.delivered", True]}]}}},                                                                                                                                                                       
                        #                             "read_count": {"$filter": {"input": "$success_stats","as": "part", "cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.read", True]}]}}},                                                                                                                                                                               
                        #                             "failed_count": {"$filter": {"input": "$failed_stats", "as": "part", "cond": {"$and": [{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}]}}} }}]))                       
                except Exception as e:
                    lg.critical("DB error - dmps_send_to_wa_analytics : " + str(e))
                    return jsonify({"id": "2004", "msg": "Data Query Error", "description": "", "data": "", "success": False})
                
                sent_count = 0
                delivered_count = 0
                read_count = 0
                failed_count = 0

                for list_count in db_list_counts :                
                    if list_count["recipient_sent_count"] != []:
                        sent_count += list_count["recipient_sent_count"]
                    if list_count["recipient_delivered_count"] != []:
                        delivered_count += list_count["recipient_delivered_count"]
                    if list_count["recipient_read_count"] != []:
                        read_count += list_count["recipient_read_count"]
                    if list_count["recipient_failed_count"] != []:
                        failed_count += list_count["recipient_failed_count"]
                        
            return jsonify({"id": "2005", "msg": "Data fetched successfully", "description": "","data":[{"template_name": str_template_name, "total_messages_count":(sent_count + failed_count) ,"sent_messages_count": (sent_count), "delivered_messages_count": (delivered_count), "read_messages_count": (read_count), "failed_messages_count": (failed_count)}] ,"total_count": {"template_name": str_template_name,"total_count":(sent_count + failed_count), "total_sent_count": (sent_count), "total_delivered_count": (delivered_count), "total_read_count": (read_count), "total_failed_count": (failed_count)},"total_cost":0.80*(sent_count + failed_count), "success": True})      

    def func_download_client_api_analytics(self,str_template_name,int_from_timestamp,int_to_timestamp,lg):
        """ Function called for downloading client API Analytics """
        try:
            db_list_counts = list(self.client_db.client_sent_to_wa_analytics_info.find({"client_number":self.client_number, "timestamp": {"$gte": int_from_timestamp, "$lte": int_to_timestamp},"template_name":str_template_name},{"_id": 0})) 
            success_stat = [doc for doc in db_list_counts if 'reason' not in doc]
            failed_stat = [doc for doc in db_list_counts if 'reason' in doc]
            lg.info(f"Success stats to get added in excel sheet are {success_stat}")
            lg.info(f"Failed stats to get added in excel sheet are {failed_stat}")
            # db_list_counts = list(self.client_db.client_sent_to_wa_analytics.aggregate([{"$match": {"client_number": self.client_number,"template_name":str_template_name}},           
            #             {"$addFields":{"sent_details": {"$filter": {"input" : "$success_stats", "as":"part","cond":{"$and":[{"$gt":["$$part.timestamp",int_from_timestamp]}, {"$lt":["$$part.timestamp",int_to_timestamp]}]}}},                                         
            #             "failed_details": {"$filter": {"input": "$failed_stats", "as": "part", "cond": {"$and": [{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}]}}} }}]))                       
        except Exception as e:
                    lg.critical("DB error - dmps_send_to_wa_analytics : " + str(e))
                    return jsonify({"id": "2004", "msg": "Data Query Error", "description": "", "data": "", "success": False})
       
        # success_stat = []
        # failed_stat = []
        if db_list_counts:
            # for single_doc in db_list_counts:
            #     success_stat.extend(single_doc["sent_details"])
            #     failed_stat.extend(single_doc["failed_details"])
            for recipient in failed_stat: 
                if isinstance(recipient['reason'], dict):
                        # error_str = recipient.get("reason", {}).get("statuses", [{}])[0].get("errors", [{}])[0].get("title", "Invalid Number")
                         lg.info(f"insider")
                         error_str = (recipient.get("reason", {})
                             .get("entry", [{}])[0]
                             .get("changes", [{}])[0]
                             .get("value", {})
                             .get("statuses", [{}])[0]
                             .get("errors", [{}])[0]
                             .get("title", "Invalid Number"))
                else:
                        error_str = recipient.get('reason','Invalid Number')


                
                success_stat.append({ "recipient_number": recipient["recipient_number"], "sent": "NA", "delivered": "NA", "read": "NA", "error": error_str})
            return jsonify({"id": "1223", "message": "Data fetched successfully", "description": "", "data": {"detailed_broadcast_stat": success_stat, "columns": ["recipient_number", "sent", "delivered", "read", "error", "button_response"]}, "success":True})
        else:
            return jsonify({"id": "1224", "message": "Data fetched successfully", "description": "", "data": {"detailed_broadcast_stat": "", "columns": ["recipient_number", "sent", "delivered", "read", "error"]}, "success": True})      
    


#-------------------------------------General Messages Analytics Start--------------------------------------------------

    def func_general_messages_analytics(self,var_response,data,str_message_type):
        """ Function called for general message analytics """
        str_recipient_number = data["recipient_number"]
        str_client_number = data["client_number"]
        resp = var_response.json()
        str_message_id = resp["messages"][0]["id"]
        current_time = int(datetime.datetime.now().timestamp()) 
        try:
            db_general_message_details = list(self.client_db.general_messages_details.find({"client_number": str_client_number}, {"_id": 0}))        
            dict_general_message_details = {"client_number": str_client_number, "text": 0, "image": 0, "audio": 0, "video": 0, "document": 0, "contact": 0,"location": 0,"sticker": 0,"spm":0,"mpm":0,"total_messages":1,"timestamp":current_time, "messages_detail":[{"message_id": str_message_id, "conversation_id": "", "conversation_type": "", "billable": True, "recipient_number": str_recipient_number, "message_type": str_message_type, "timestamp": current_time, "sent":False, "delivered": False, "read": False}],"failed_stats":[]}                                                          
            if db_general_message_details == []:
                dict_general_message_details = {"client_number": str_client_number, "text": 0, "image": 0, "audio": 0, "video": 0, "document": 0, "contact": 0,"location": 0,"sticker": 0,"spm":0,"mpm":0,"total_messages":1,"timestamp":current_time, "messages_detail":[{"message_id": str_message_id, "conversation_id": "", "conversation_type": "", "billable": True, "recipient_number": str_recipient_number, "message_type": str_message_type, "timestamp": current_time, "sent":False, "delivered": False, "read": False}],"failed_stats":[]}                                               
                dict_general_message_details.update({str_message_type:1})

                self.client_db.general_messages_details.insert_one(dict_general_message_details)                                                    
            else:            
                int_total_messages = db_general_message_details[-1]["total_messages"]
                if int_total_messages == 100:
                    dict_general_message_details = {"client_number": str_client_number, "text": 0, "image": 0, "audio": 0, "video": 0, "document": 0, "contact": 0,"location": 0, "sticker": 0,"spm":0,"mpm":0,"total_messages":1, "timestamp":current_time, "messages_detail":[{"message_id": str_message_id, "conversation_id": "", "conversation_type": "", "billable": True, "recipient_number": str_recipient_number, "message_type": str_message_type, "timestamp": current_time, "sent":False, "delivered": False, "read": False}],"failed_stats":[]}                                               
                    dict_general_message_details.update({str_message_type:1})
                    self.client_db.general_messages_details.insert_one(dict_general_message_details)
                else:               
                    self.client_db.general_messages_details.update_one({"client_number": str_client_number,"total_messages": {"$lt":100}},{"$push": {"messages_detail": {"message_id": str_message_id, "conversation_id": "", "conversation_type": "", "billable": True, "recipient_number": str_recipient_number, "message_type": str_message_type, "timestamp": current_time, "sent": False, "delivered": False, "read": False}},"$inc": {str(str_message_type): 1,"total_messages":1}})
        except Exception as e:
            return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
        

    def func_general_messages_invalid(self,response,data):  
        """Call this function when invalid contact while sending general messages"""
        str_error = response.json()["errors"][0]["title"]
        str_recipient_number =  data["recipient_number"]
        str_client_number = data["client_number"] 
        current_timestamp =int(datetime.datetime.now().timestamp())
        
        try:
            # ew_db=ClsMongoDBInit.get_db_client()
            db_general_messages_details = list(self.client_db.general_messages_details.find({"client_number": str_client_number}, {"_id":0}))
            if db_general_messages_details == []:
                dict_general_message_details = {"client_number": str_client_number, "text": 0, "image": 0, "audio": 0, "video": 0, "document": 0, "contact": 0,"location": 0, "sticker": 0,"spm":0,"mpm":0,"total_messages":1,"timestamp":current_timestamp, "messages_detail":[],"failed_stats":[{"recipient_number": str_recipient_number,"error":str_error,"timestamp": current_timestamp}]}
                self.client_db.general_messages_details.insert_one(dict_general_message_details)   
            else:
                self.client_db.general_messages_details.update_one({"client_number":str_client_number, "total_messages": {"$lt":100}},{"$push":{"failed_stats":{"recipient_number": str_recipient_number,"error":str_error,"timestamp": current_timestamp}}})
        except Exception as e:
            return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})                
       
        return {"response":"message sent failed"}
    

    def func_get_general_messages_analytics(self,int_from_timestamp,int_to_timestamp,bot_id,lg):
        """ Function called for genaral message analytics """
        try:
            db_list_counts = list(self.client_db.general_messages_details.aggregate([ {"$match": {"client_number": self.client_number,"timestamp":{"$gt":int_from_timestamp,"$lt":int_to_timestamp}}},           
                        {"$addFields":{"text_messages_count": {"$filter": {"input": "$messages_detail","as": "part", "cond":  {"$and":[ {"$gt": ["$$part.timestamp", int_from_timestamp]},{"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.message_type", "text"]}]}}},                    
                                        "image_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.message_type", "image"]}]}}},                
                                        "audio_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.message_type", "audio"]}]}}},
                                        "video_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.message_type", "video"]}]}}},
                                        "document_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.message_type", "document"]}]}}},
                                        "contact_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.message_type", "contact"]}]}}},
                                        "location_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.message_type", "location"]}]}}},
                                        "sent_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.sent", True]}]}}},
                                        "delivered_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.delivered", True]}]}}},
                                        "read_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.read", True]}]}}},
                                        "sticker_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.message_type", "sticker"]}]}}},
                                        "spm_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.message_type", "spm"]}]}}},
                                        "mpm_messages_count": {"$filter": {"input": "$messages_detail","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}, {"$eq": ["$$part.message_type", "mpm"]}]}}},
                                        "failed_messages_count": {"$filter": {"input": "$failed_stats","as": "part","cond": {"$and":[{"$gt": ["$$part.timestamp", int_from_timestamp]}, {"$lt": ["$$part.timestamp", int_to_timestamp]}]}}} }}]))
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - general_messages_details : " + str(e))
            return jsonify({"id": "2112", "msg": "Data Query Error", "success": False})
        

        text_messages_count = 0   
        image_messages_count = 0 
        audio_messages_count = 0   
        video_messages_count = 0
        document_messages_count = 0   
        location_messages_count = 0 
        contact_messages_count = 0   
        sticker_messages_count = 0    
        sent_messages_count = 0 
        delivered_messages_count = 0
        read_messages_count = 0
        failed_messages_count = 0
        spm_messages_count = 0
        mpm_messages_count = 0

        for list_count in db_list_counts :          
            if list_count["text_messages_count"] != []:
                text_messages_count += len(list_count["text_messages_count"])
            if list_count["image_messages_count"] != []:
                image_messages_count += len(list_count["image_messages_count"])
            if list_count["audio_messages_count"] != []:
                audio_messages_count += len(list_count["audio_messages_count"])
            if list_count["video_messages_count"] != []:
                video_messages_count += len(list_count["video_messages_count"])
            if list_count["document_messages_count"] != []:
                document_messages_count += len(list_count["document_messages_count"])
            if list_count["location_messages_count"] != []:
                location_messages_count += len(list_count["location_messages_count"])
            if list_count["contact_messages_count"] != []:
                contact_messages_count += len(list_count["contact_messages_count"])
            if list_count["sticker_messages_count"] != []:
                sticker_messages_count += len(list_count["sticker_messages_count"])
            if list_count["sent_messages_count"] != []:
                sent_messages_count += len(list_count["sent_messages_count"])
            if list_count["delivered_messages_count"] != []:
                delivered_messages_count += len(list_count["delivered_messages_count"])
            if list_count["read_messages_count"] != []:
                read_messages_count += len(list_count["read_messages_count"])
            if list_count["spm_messages_count"] != []:
                spm_messages_count += len(list_count["spm_messages_count"])
            if list_count["mpm_messages_count"] != []:
                mpm_messages_count += len(list_count["mpm_messages_count"])
            if list_count["failed_messages_count"] != [] or list_count["failed_messages_count"] != None :
                failed_messages_count += len(list_count["failed_messages_count"])
           
        total_messages = text_messages_count + image_messages_count + audio_messages_count + video_messages_count + document_messages_count + location_messages_count + contact_messages_count + sticker_messages_count + failed_messages_count + spm_messages_count + mpm_messages_count

        return jsonify({"response":{"data": {"text":(text_messages_count),"image":(image_messages_count),"document":(document_messages_count),"audio":(audio_messages_count),"video":(video_messages_count),"contact":(contact_messages_count),"location":(location_messages_count),"sticker":(sticker_messages_count),"spm":spm_messages_count,"mpm":mpm_messages_count,"total_messages": {"total_messages_count": total_messages, "total_sent_count":sent_messages_count, "total_delivered_count": delivered_messages_count, "total_read_count": read_messages_count,"failed_messages_count":failed_messages_count} }},"success":True})


class ClsStatusUpdate():
    """ Class for webhook status (sent,delivered,read) updates"""

    def __init__(self,client_number):
        """ Class for webhook status (sent,delivered,read) updates

        Initialize required object and variables:

        Parameters:
            client_number (str): Mobile Number of WhatsApp 
            db_client_waba_settings (dict): All WABA Settings         
        Local Variable:
            client_db_name(str): Fetch and generate DB name for individual client
            ew_db(DB object): Initialize DB
            client_db(DB Object):  Initialize DB
            db_dmps_send_to_wa_analytics (list): list of documents in dmps_send_to_wa_analytics collection
            db_general_message_details (list): list of documents in dmps_send_to_wa_analytics collection
            str_message_id (str): unique id for the message or template sent
            str_conversation_id (str) : field in webhook data received
            str_billable (str): field in webhook data received
            str_conversation_category (str): field in webhook data received
        Returns:
            All above parameteres and variables
      
        """
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        obj_common = ClsCommon()
        self.client_number = client_number
        db_client_waba_settings = obj_common.get_waba_settings_by_cc_client_number(self.client_number)
        lg.info(f"dev{db_client_waba_settings}")
        if "error" in db_client_waba_settings:
            return jsonify({"error": "Invalid client number. Please enter appropriate client number"})    
        self.client_db_name = str(db_client_waba_settings["response"]["ew_id"]).lower() + "_" + db_client_waba_settings["response"]["waba_id"]       
        self.client_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)
        

    def func_sent_status(self,data):
        """ This function is called when sent status is received on webhook """
        str_message_id = data['statuses'][0]['id']
        str_conversation_id = data["statuses"][0]["conversation"]["id"]
        str_billable = data["statuses"][0]["pricing"]["billable"]
        str_conversation_category = data["statuses"][0]["pricing"]["category"]
        try:
            db_dmps_send_to_wa_analytics_doc = self.client_db.dmps_send_to_wa_analytics.find_one_and_update({"success_stats.message_id": str_message_id},{"$set": {"success_stats.$.sent":True,"success_stats.$.conversation_id":str_conversation_id,"success_stats.$.billable":str_billable,"success_stats.$.conversation_type":str_conversation_category},"$inc":{"recipient_sent_count":1}})                  
            if db_dmps_send_to_wa_analytics_doc == None:
                db_general_messages_details = self.client_db.general_messages_details.find_one_and_update({"messages_detail.message_id": str_message_id},{"$set": {"messages_detail.$.sent":True,"messages_detail.$.conversation_id":str_conversation_id,"messages_detail.$.billable":str_billable,"messages_detail.$.conversation_type":str_conversation_category}})   
            if db_dmps_send_to_wa_analytics_doc == None and db_general_messages_details == None:
                db_broadcast_details_doc = self.client_db.broadcast_details.find_one_and_update({"success_stats.message_response_id": str_message_id},{"$set": {"success_stats.$.sent":True},"$inc":{"recipient_sent_count":1}})               
            if db_dmps_send_to_wa_analytics_doc == None and (db_general_messages_details == None and db_broadcast_details_doc == None):
                self.client_db.client_sent_to_wa_analytics.find_one_and_update({"success_stats.message_id": str_message_id},{"$set": {"success_stats.$.sent":True,"success_stats.$.conversation_id":str_conversation_id,"success_stats.$.billable":str_billable,"success_stats.$.conversation_type":str_conversation_category},"$inc":{"recipient_sent_count":1}})                    
        except Exception as e:
            return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
        

    def func_delivered_status(self,data,msg_id,lst_statuses):
        """ This function is called when delivered status is received on webhook """
        from waapi.celery_worker3 import async_updateanalytics_pending
        str_message_id = msg_id
        lg.info(f"message_id for deliver is {str_message_id}")
        # str_conversation_id = data["statuses"][0]["conversation"]["id"]
        # lst_statuses = data['statuses'][0]
        lst_statuses = lst_statuses

        lg.info(f"lst_statuses value is {lst_statuses}")
        if 'conversation' in lst_statuses:
            # str_conversation_id = data["statuses"][0]["conversation"]["id"]
            str_conversation_id = lst_statuses["conversation"]["id"]

            lg.info(f"str_conversation_id is {str_conversation_id}")
        else:
            str_conversation_id = " "
            lg.info(f"str_conversation_id is {str_conversation_id}")
        if 'pricing' in lst_statuses:
            # str_billable = data["statuses"][0]["pricing"]["billable"]
            str_billable = lst_statuses["pricing"]["billable"]
            str_conversation_category = lst_statuses["pricing"]["category"]
            lg.info(f"str_billable is {str_billable}")
            lg.info(f"str_conversation_category is {str_conversation_category}")

        # str_billable = data["statuses"][0]["pricing"]["billable"]
        # str_conversation_category = data["statuses"][0]["pricing"]["category"]
        else:
            str_billable = " "
            str_conversation_category = " "
        lg.info(f"whatsapp template analytics doc{data}")
        try:            
            try:
                db_dmps_send_to_wa_analytics_doc = self.client_db.dmps_send_to_wa_analytics.find_one_and_update({"success_stats.message_id": str_message_id},{"$set":  {"success_stats.$.delivered":True,"success_stats.$.sent":True,"success_stats.$.conversation_id":str_conversation_id,"success_stats.$.billable":str_billable,"success_stats.$.conversation_type":str_conversation_category},"$inc":{"recipient_delivered_count":1,"recipient_sent_count":1}})
                lg.info(f"db_dmps_send_to_wa_analytics {db_dmps_send_to_wa_analytics_doc}")
            #lg.info("db_dmps_send_to_wa_analytics : " + db_client_send_to_wa_analytics_doc)
            except Exception as e:
                lg.critical(f"error at line 498 inside deliver is {e}")

            if db_dmps_send_to_wa_analytics_doc == None:
                db_general_messages_details = self.client_db.general_messages_details.find_one_and_update({"messages_detail.message_id": str_message_id},{"$set": {"messages_detail.$.delivered":True,"success_stats.$.sent":True,"messages_detail.$.conversation_id":str_conversation_id,"messages_detail.$.billable":str_billable,"messages_detail.$.conversation_type":str_conversation_category}})
                # lg.info(f"inside first if of delivered_status {db_general_messages_details}")
            if db_dmps_send_to_wa_analytics_doc == None and db_general_messages_details == None:
                # db_broadcast_details_doc = self.client_db.broadcast_details_info.insert_one({"message_response_id": str_message_id,"delivered":True})
                # db_broadcast_details = self.client_db.broadcast_details_info.find_one_and_update({"message_response_id": str_message_id},{"$set":{"delivered":True}},upsert=True)
                # db_broadcast_details_filter = self.client_db.broadcast_details_info.find_one({"message_response_id": str_message_id})
                try:
                    db_broadcast_details_filter = self.client_db.broadcast_details_info.find_one({"message_response_id": str_message_id,"broadcast_id": {"$exists": True}})
                    lg.info(f"db broadcast detials filer inside delivered_status  is  {db_broadcast_details_filter} kd")
                except Exception as e:
                    lg.critical("error while quering inside delivered status is at line 507 {e}")
                if db_broadcast_details_filter:
                    # lg.info(f"{db_broadcast_details_filter.get('delivered',False)}")
                    if db_broadcast_details_filter.get("delivered",False) != True:
                        lg.info("inside if for delivered")
                        db_broadcast_ = self.client_db.broadcast_details.find_one_and_update({"broadcast_id": db_broadcast_details_filter["broadcast_id"]},{"$inc":{"recipient_delivered_count":1}})                  
                        # lg.info(f"inside second if of delivered_status{db_broadcast_}")
                        db_broadcast_ = self.client_db.broadcast_details_info.find_one_and_update({"message_response_id": str_message_id,"broadcast_id": db_broadcast_details_filter["broadcast_id"]},{"$set":{"delivered":True,"data_delivered":data}})                  
                    
                    elif db_broadcast_details_filter.get("delivered","") == True:
                        pass
                else:
                    # lg.info(f"i am in else,{data}")
                    try:
                        # tasksd = async_updateanalytics_pending.apply_async(args=(self.client_number,data),countdown =100)
                        #existing_task = async_updateanalytics_pending.AsyncResult(str_message_id)
                        if True:#not existing_task or existing_task.status not in ['PENDING', 'STARTED']:
                            try:
                                if data.get("counter",0)>=0:
                                    self.client_db.unknown_delivered.insert_one({"client_number":self.client_number,"data":data})
                                else:
                                    data["counter"] = data.get("counter",0)+1
                                    tasksd = async_updateanalytics_pending.apply_async(args=(self.client_number,data),countdown=300)
                                    lg.info(f"task_created for message_response_id {str_message_id} with task_id {tasksd.id}")
                                    result_job = async_updateanalytics_pending.AsyncResult(tasksd.id)
                                    # lg.info(f"Task Status: {result_job.status}")
                                    lg.info(f"else task is {tasksd.id} Task Status: {result_job.status} args data is {self.client_number}{data}")
                                    # if result_job.successful():
                                    #     lg.info("inside job sucess if")
                                    result = {"success": "True", "msg": "Async request started","taskid":tasksd.id}
                                    lg.info("dev3")
                                    return result
                            except Exception as e:
                                lg.critical("analytics celery_pending failed: " + str(e))
                                return jsonify({"error": {"id": "1232", "message": "Something went wrong in payload or result"}, "success": "False"})
                        # else:
                        #     self.client_db.unknown_status.insert_one({"client_number":self.client_number,"data":data})
                        # #     lg.info(f"existing task found  for message_response_id {str_message_id} and status is {existing_task.status} ")
                        #     from wacore.pkg_webhook.mod_webhook_functions import ClsWebhook
                        #     obj_webhook = ClsWebhook(self.client_number)
                        #     obj_webhook.func_webhook(data)

                    except Exception as e:
                        lg.critical("analytics celery failed: " + str(e))
                        return jsonify({"error": {"id": "1231", "message": "Something went wrong in payload or result"}, "success": "False"})

                    else:
                        pass
            if db_dmps_send_to_wa_analytics_doc == None and db_general_messages_details == None and db_broadcast_details_filter == None:
                db_clientapi_details_filter = self.client_db.client_sent_to_wa_analytics_info.find_one({"message_response_id": str_message_id,"template_name": {"$exists": True}})
                if db_clientapi_details_filter:
                    current_timestamp = int(datetime.datetime.now().timestamp())
                    dt_object_cur = datetime.datetime.utcfromtimestamp(current_timestamp)
                    date_cur = dt_object_cur.strftime("%d")
                    month_cur = dt_object_cur.strftime("%m")
                    year_cur = dt_object_cur.strftime("%Y")
                    datemonth_cur = str(date_cur+month_cur+year_cur)
                    if db_clientapi_details_filter.get("delivered",False) != True:
                        db_client_api = self.client_db.client_sent_to_wa_analytics.find_one_and_update({"template_name": db_clientapi_details_filter['template_name'],"date_month":datemonth_cur,"client_number": self.client_number},{"$inc": {"recipient_delivered_count":1}})
                        db_client_api_ = self.client_db.client_sent_to_wa_analytics_info.find_one_and_update({"message_response_id": str_message_id,"template_name":db_clientapi_details_filter['template_name']},{"$set":{"delivered":True,"data_delivered":data}})
                    elif db_clientapi_details_filter.get("delivered","") == True:
                        pass

                else:
                    pass
                # self.client_db.client_sent_to_wa_analytics.find_one_and_update({"success_stats.message_id": str_message_id},{"$set": {"success_stats.$.delivered":True,"success_stats.$.sent":True,"success_stats.$.conversation_id":str_conversation_id,"success_stats.$.billable":str_billable,"success_stats.$.conversation_type":str_conversation_category},"$inc":{"recipient_delivered_count":1,"recipient_sent_count":1}})                     
        except Exception as e:
            lg.info(f"expection inside delivered_status is {e}")
            return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": "False","exp":e})
        

    def func_read_status(self,data,msg_id,lst_statuses):
        """ This function is called when read status is received on webhook """
        # str_message_id = data['statuses'][0]['id']
        str_message_id = msg_id
        lg.info(f"message_id for read is {str_message_id} and data is {data}")
        lg.info("whatsapp template analytics doc for read_status : " + str_message_id)
        try:
            db_dmps_send_to_wa_analytics_doc = self.client_db.dmps_send_to_wa_analytics.find_one_and_update({"success_stats.message_id": str_message_id},{"$set": {"success_stats.$.read":True,"success_stats.$.delivered":True},"$inc":{"recipient_read_count":1}})
            # lg.info(f"db_dmps_send_to_wa_analytics for read_status is :  {db_dmps_send_to_wa_analytics_doc}")
            if db_dmps_send_to_wa_analytics_doc == None:
                try:
                    db_general_messages_details = self.client_db.general_messages_details.find_one_and_update({"messages_detail.message_id": str_message_id},{"$set": {"messages_detail.$.read":True,"messages_detail.$.delivered":True}})       
                # lg.info(f"db_dmps_send_to_wa_analytics for read_status inside first if is :   {db_general_messages_details}")              
                except Exception as e:
                    lg.info(f"error at line 578 inside read is {e} ")
            if db_dmps_send_to_wa_analytics_doc == None and db_general_messages_details == None:
                 db_client_send_to_wa_analytics_doc = self.client_db.client_sent_to_wa_analytics_info.find_one({"message_response_id": str_message_id,"template_name": {"$exists": True}})
                 if db_client_send_to_wa_analytics_doc:
                    current_timestamp = int(datetime.datetime.now().timestamp())
                    dt_object_cur = datetime.datetime.utcfromtimestamp(current_timestamp)
                    date_cur = dt_object_cur.strftime("%d")
                    month_cur = dt_object_cur.strftime("%m")
                    year_cur = dt_object_cur.strftime("%Y")
                    datemonth_cur = str(date_cur+month_cur+year_cur)
                    if db_client_send_to_wa_analytics_doc.get("delivered",False) == False:
                         db_client_api = self.client_db.client_sent_to_wa_analytics.find_one_and_update({"template_name": db_client_send_to_wa_analytics_doc['template_name'],"date_month":datemonth_cur ,"client_number": self.client_number},{"$inc": {"recipient_read_count":1,"recipient_delivered_count":1}})
                         db_client_api = self.client_db.client_sent_to_wa_analytics_info.find_one_and_update({"message_response_id": str_message_id,"template_name":db_client_send_to_wa_analytics_doc['template_name']},{"$set":{"read":True,"data_read":data,"delivered":True}})
                    else:
                        db_client_api = self.client_db.client_sent_to_wa_analytics.find_one_and_update({"template_name": db_client_send_to_wa_analytics_doc['template_name'],"date_month":datemonth_cur,"client_number": self.client_number },{"$inc": {"recipient_read_count":1}})
                        db_client_api = self.client_db.client_sent_to_wa_analytics_info.find_one_and_update({"message_response_id": str_message_id,"template_name":db_client_send_to_wa_analytics_doc['template_name']},{"$set":{"read":True,"data_read":data}})
                 else:
                    pass
            
            if db_dmps_send_to_wa_analytics_doc == None and db_general_messages_details == None and db_client_send_to_wa_analytics_doc == None:
                #db_broadcast_details = self.client_db.broadcast_details.find_one_and_update({"success_stats.message_response_id": str_message_id},{"$set": {"success_stats.$.read":True},"$inc":{"recipient_read_count":1}})
                # lg.info(f"{str_message_id} str message id")
                lg.info("inside thrid if of read")
                try:
                    db_broadcast_details = self.client_db.broadcast_details_info.find_one({"message_response_id": str_message_id,"broadcast_id": {"$exists": True}})
                except Exception as e:
                    lg.info(f"error at line 589 inside read is {e} ")
                #db_broadcast_details = self.client_db.broadcast_details_info.find_one_and_update({"message_response_id": str_message_id},{"$set": {"read":True}},upsert=True)
                
                if db_broadcast_details:
                    lg.info("inside db_broadcast_details if condition")
                    db_broadcast_ = self.client_db.broadcast_details.find_one_and_update({"broadcast_id": db_broadcast_details["broadcast_id"]},{"$inc":{"recipient_read_count":1}})                 
                    db_broadcast_details_read = self.client_db.broadcast_details_info.find_one_and_update({"message_response_id": str_message_id,"broadcast_id": db_broadcast_details["broadcast_id"]},{"$set": {"read":True,"data_read":data}})
                    lg.info(f"db_broadcast_details_read is {db_broadcast_details_read}")
                    db_broadcast_details_read = self.client_db.broadcast_details_info.find_one({"broadcast_id": db_broadcast_details["broadcast_id"],"message_response_id": str_message_id})
                    if db_broadcast_details_read["read"] == True and db_broadcast_details_read.get("delivered",False) == False:
                        db_broadcast_details = self.client_db.broadcast_details_info.find_one_and_update({"message_response_id": str_message_id,"broadcast_id": db_broadcast_details["broadcast_id"]},{"$set": {"delivered":True,"data_read":data}})
                        db_broadcastf = self.client_db.broadcast_details.find_one_and_update({"broadcast_id": db_broadcast_details["broadcast_id"]},{"$inc":{"recipient_delivered_count":1}})
                        # lg.info(f"inside delivered skipped condition is {db_broadcast_details}")
                else:
                    try:
                        from waapi.celery_worker3 import async_updateanalytics_pending
                        #existing_task = async_updateanalytics_pending.AsyncResult(str_message_id)
                        if True:#not existing_task or existing_task.status not in ['PENDING', 'STARTED']:
                            try:
                                if data.get("counter",0)>=0:
                                    self.client_db.unknown_read.insert_one({"client_number":self.client_number,"data":data})
                                else:
                                    data["counter"] = data.get("counter",0)+1
                                    tasksd = async_updateanalytics_pending.apply_async(args=(self.client_number,data),countdown=300)
                                    # lg.info(f"else task is {tasksd.id} is")
                                    lg.info(f"task_created for message_response_id {str_message_id} with task_id {tasksd.id}")
                                    result_job = async_updateanalytics_pending.AsyncResult(tasksd.id)
                                    # lg.info(f"Task Status: {result_job.status}")
                                    lg.info(f"else task is task_id is   {tasksd.id} ,  Task Status: {result_job.status} args data is {self.client_number} {data}")
                                    # if result_job.successful():
                                    #     lg.info("inside job sucess if")
                                    result = {"success": "True", "msg": "Async request started","taskid":tasksd.id}
                                    lg.info("dev3")
                                    return result
                            except Exception as e:
                                lg.critical("analytics celery_pending failed: " + str(e))
                                return jsonify({"error": {"id": "1232", "message": "Something went wrong in payload or result"}, "success": "False"})
                        # else:
                        #     self.client_db.unknown_status.insert_one({"client_number":self.client_number,"data":data})
                        # #     lg.info(f"existing  task found for message_response_id {str_message_id} and status is {existing_task.status} ")
                        #     from wacore.pkg_webhook.mod_webhook_functions import ClsWebhook
                        #     obj_webhook = ClsWebhook(self.client_number)
                        #     obj_webhook.func_webhook(data)


                    except Exception as e:
                        lg.critical("analytics celery failed: " + str(e))
                        return jsonify({"error": {"id": "1231", "message": "Something went wrong in payload or result"}, "success": "False"})

                    # elif db_broadcast_details_filter["read"] == True :
                    #     db_broadcast_details = self.client_db.broadcast_details_info.find_one_and_update({"message_response_id": str_message_id},{"$set": {"delivered":True}})
                    #     # db_broadcastf = self.client_db.broadcast_details.find_one_and_update({"broadcast_id": db_broadcast_details["broadcast_id"]},{"$inc":{"recipient_delivered_count":1}})
                        # db_broadcastf = self.client_db.broadcast_details.find_one_and_update({"broadcast_id": db_broadcast_details["broadcast_id"]},{"$inc":{"recipient_delivered_count":1}})
                    # lg.info(f"db_dmps_send_to_wa_analytics for read_status inside thrid if is :  {db_broadcast_details}")                             
                    #element = db_broadcast_details["success_stats"]
                    #if db_broadcast_details:
                    #   for recipient in element:
                    #        if recipient["message_response_id"] == str_message_id:
                    #            if recipient["delivered"] == False:
                    #                lg.info("updating the read status when delivered status is false" + str(str_message_id))
                                    #self.client_db.broadcast_details.update_one({"success_stats.message_response_id": str_message_id},{"$set": {"success_stats.$.delivered":True,"success_stats.$.sent":True},"$inc":{"recipient_delivered_count":1,"recipient_sent_count":1}})  
                                    # vishu to figureoutself.client_db.broadcast_details.update_one({"broadcast_id": int_broadcast_id }, {"$push": {"success_stats":{"recipient_number":str_recipient_number,"message_response_id":message_response_id,"sent":False,"delivered":False,"read":False,"button_response":"NA","dynamic_variables":var_message_body['template']['components']} }})                                         
        except Exception as e:
            lg.info(f"expec {e}")                             

            return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
    def func_delivered_failed(self,data,message_id):
        # str_message_id =  data['statuses'][0]['id']
        str_message_id = message_id
        # lg.info(f"client number is {self.client_number}")
        lg.info(f"message_id for failed_deliver  is {str_message_id} and data is {data}")
        db_client_send_to_wa_analytics_doc = self.client_db.client_sent_to_wa_analytics_info.find_one({"message_response_id": str_message_id,"template_name": {"$exists": True}})

        if db_client_send_to_wa_analytics_doc:
            current_timestamp = int(datetime.datetime.now().timestamp())
            dt_object_cur = datetime.datetime.utcfromtimestamp(current_timestamp)
            date_cur = dt_object_cur.strftime("%d")
            month_cur = dt_object_cur.strftime("%m")
            year_cur = dt_object_cur.strftime("%Y")
            datemonth_cur = str(date_cur+month_cur+year_cur)
            try:
                db_broadcast_details = self.client_db.client_sent_to_wa_analytics_info.find_one({"message_response_id": str_message_id,"template_name": {"$exists": True}})
                lg.info(f"db_broadcast_details for failed_deliver is {db_broadcast_details}")
            except Exception as e:
                lg.info(f"error inside deliver_failed")
            # lg.info(f" broadcast_id is {b_id}")
            if 'delivered' not in db_broadcast_details:
                self.client_db.client_sent_to_wa_analytics_info.find_one_and_update({"message_response_id": str_message_id,"template_name": db_broadcast_details["template_name"],"date_month":db_broadcast_details["date_month"]},{"$set": {"sent":"NA","reason": data}})
                self.client_db.client_sent_to_wa_analytics.find_one_and_update( {"template_name": db_broadcast_details["template_name"],"date_month":db_broadcast_details["date_month"]},{"$inc": {"recipient_failed_count": 1, "recipient_sent_count": -1}})
            # else:
            #      self.client_db.broadcast_details_info.find_one_and_update({"message_response_id": str_message_id,"broadcast_id": db_broadcast_details["broadcast_id"]},{"$set": {"sent":"NA","reason": data}})
            #      self.client_db.broadcast_details.find_one_and_update( {"broadcast_id": db_broadcast_details["broadcast_id"]},{"$inc": {"recipient_failed_count": 1, "recipient_sent_count": -1,"recipient_delivered_count":-1}})
        else:
            pass
        try:
            db_broadcast_details = self.client_db.broadcast_details_info.find_one({"message_response_id": str_message_id,"broadcast_id": {"$exists": True}})
            lg.info(f"db_broadcast_details for failed_deliver is {db_broadcast_details}")
        except Exception as e:
            lg.info(f"error inside deliver_failed")
        if db_broadcast_details:
            b_id = db_broadcast_details["broadcast_id"]
            # lg.info(f" broadcast_id is {b_id}")
            if 'delivered' not in db_broadcast_details:
                self.client_db.broadcast_details_info.find_one_and_update({"message_response_id": str_message_id,"broadcast_id": db_broadcast_details["broadcast_id"]},{"$set": {"sent":"NA","reason": data}})
                self.client_db.broadcast_details.find_one_and_update( {"broadcast_id": db_broadcast_details["broadcast_id"]},{"$inc": {"recipient_failed_count": 1, "recipient_sent_count": -1}})
            else:
                 self.client_db.broadcast_details_info.find_one_and_update({"message_response_id": str_message_id,"broadcast_id": db_broadcast_details["broadcast_id"]},{"$set": {"sent":"NA","reason": data}})
                 self.client_db.broadcast_details.find_one_and_update( {"broadcast_id": db_broadcast_details["broadcast_id"]},{"$inc": {"recipient_failed_count": 1, "recipient_sent_count": -1,"recipient_delivered_count":-1}})
        else:
            try:
                from waapi.celery_worker3 import async_updateanalytics_pending
                #existing_task = async_updateanalytics_pending.AsyncResult(str_message_id)
                if True:#not existing_task or existing_task.status not in ['PENDING', 'STARTED']:
                    try:
                        if data.get("counter",0)>=0:
                            self.client_db.unknown_failed.insert_one({"client_number":self.client_number,"data":data})
                        else:
                            data["counter"] = data.get("counter",0)+1
                            tasksd = async_updateanalytics_pending.apply_async(args=(self.client_number,data),countdown=300)
                            # lg.info(f"else task is {tasksd.id} is")
                            lg.info(f"task_created for message_response_id {str_message_id} with task_id {tasksd.id}")
                            result_job = async_updateanalytics_pending.AsyncResult(tasksd.id)
                            # lg.info(f"Task Status: {result_job.status}")
                            lg.info(f"else task is task_id is   {tasksd.id} ,  Task Status: {result_job.status} args data is {self.client_number} {data}")
                            # if result_job.successful():
                            #     lg.info("inside job sucess if")
                            result = {"success": "True", "msg": "Async request started","taskid":tasksd.id}
                            lg.info("dev3")
                            return result
                    except Exception as e:
                        lg.critical("analytics celery_pending failed: " + str(e))
                        return jsonify({"error": {"id": "7778", "message": "Something went wrong in payload or result"}, "success": "False"})
            except Exception as e:
                    lg.critical("analytics celery_pending failed: " + str(e))
                    return jsonify({"error": {"id": "7779", "message": "Something went wrong in delivered_failed status "}, "success": "False"})
    def vish_message_test(data):
        if data:
            lg.info(f"vish log for data for message replied {data}")
            return data
        else:
            lg.info(f"data is empty")
            return data    




class ClsButtonResponse():
    """ Class for recording button responses from users updates """
    def __init__(self,client_number):
        """ Class for recording button responses from users updates
            
        Initialize required object and variables:

        Parameters:
            client_number (str): Mobile Number of WhatsApp 
            db_client_waba_settings (dict): All WABA Settings         
        Local Variable:
            client_db_name(str): Fetch and generate DB name for individual client
            ew_db(DB object): Initialize DB
            client_db(DB Object):  Initialize DB
            db_dmps_send_to_wa_analytics (list): list of documents in dmps_send_to_wa_analytics collection
            db_general_message_details (list): list of documents in dmps_send_to_wa_analytics collection
            str_message_id (str) : unique id for the message or template sent
            button_response (str) :  button response from user received via webhook
        Returns:
            All above parameteres and variables
      
        """
        obj_common = ClsCommon()
        self.client_number = client_number
        db_client_waba_settings = obj_common.get_waba_settings_by_cc_client_number(self.client_number)
        if "error" in db_client_waba_settings:
            return jsonify({"error": "Invalid client number. Please enter appropriate client number"})    
        self.client_db_name = db_client_waba_settings["response"]["ew_id"].lower() + "_" + db_client_waba_settings["response"]["waba_id"]
   
        self.client_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)
        

    def func_button_response(self,data,button_id,button_text):
        """This function called to record button response from users"""
        lg.info(f"value of data for button_response is {data}")
        # button_response = data["messages"][0]["button"]["text"]
        button_response = button_text
        lg.info(f"button_response from user is {button_response}")
        # str_message_id = data["messages"][0]["context"]["id"]
        str_message_id = button_id

        #ew_db.button_response.update_one({"message_id": message_id2},{"$push": {"user_response":button_response}})
        try:
            lg.info(f"Inside button_response function with message_id is {str_message_id}")
            # self.client_db.dev.insert_one({"dev":"dev"})
            # self.client_db.broadcast_details_info.update_one({"message_response_id": str_message_id},{"$set": {"button_response":button_response}}) #starting one which is working
            # self.client_db.broadcast_details_info.update_one({"message_response_id": str_message_id},{"$addToSet": {"button_responses": button_response}})  #next change working but not getting added in excel
            
            existing_button_response = self.client_db.broadcast_details_info.find_one({"message_response_id": str_message_id},{"button_response": 1})

            if existing_button_response:
                existing_button_response = existing_button_response.get("button_response", "")
            updated_button_response = ",".join(filter(None, [existing_button_response, button_response]))

            self.client_db.broadcast_details_info.update_one({"message_response_id": str_message_id}, {"$set": {"read":True, "button_response": updated_button_response}})

            self.client_db.client_sent_to_wa_analytics_info.update_one({"message_response_id": str_message_id}, {"$set": {"read":True, "button_response": updated_button_response}})
            # self.client_db.client_sent_to_wa_analytics.update_one({"success_stats.message_id": str_message_id},{"$set": {"success_stats.$.button_response":button_response}})
            self.client_db.dmps_send_to_wa_analytics.update_one({"success_stats.message_id": str_message_id},{"$set": {"success_stats.$.button_response":button_response}})
            # -------- For Kasi only start --------------------------
            
            if button_response in ["NO", "No", "no"]:
                db_broadcast_details = self.client_db.broadcast_details.find_one({"success_stats.message_response_id": str_message_id, "template_name": "consent_management"})
                if db_broadcast_details != None:
                    lg.info("inside if for button response")
                    self.client_db.concent_details.update_one({"recipient_number": int(data["contacts"][0]["wa_id"])}, {"$set": {"concent_flag": False}})
                else:
                    lg.info("inside else for button response")
                    db_dmps_send_to_wa_analytics = self.client_db.dmps_send_to_wa_analytics.find_one({"success_stats.message_id": str_message_id, "template_name": "consent_management"})
                    if db_dmps_send_to_wa_analytics != None:
                        self.client_db.concent_details.update_one({"recipient_number": int(data["contacts"][0]["wa_id"])}, {"$set": {"concent_flag": False}})

            if button_response == "Checkout My Order":
                url = "https://botbuilder.engagely.ai/services_dev/kasi/add_product"
                #button_data = {"user_reply":"Checkout My Order"}
                try:
                    requests.request("POST", url, json=data)
                except:
                    pass
            if button_response == "Cancel My Order":
                url = "https://botbuilder.engagely.ai/services_dev/kasi/add_product"
                #button_data = {"user_reply":"Cancel My Order"}
                try:
                    requests.request("POST", url, json=data)
                except:
                    pass
            # -------- For Kasi only end --------------------------
        except Exception as e:
            return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
         

class ClsBroadcastAnalysis():
    """ Class for broadcast analytics """
    def __init__(self,client_number):
        """ Class for broadcast analytics
            
        Initialize required object and variables:

        Parameters:
            client_number (str): Mobile Number of WhatsApp 
            db_client_waba_settings (dict): All WABA Settings         
        Local Variable:
            client_db_name(str): Fetch and generate DB name for individual client
            ew_db(DB object): Initialize DB
            client_db(DB Object):  Initialize DB
            db_dmps_send_to_wa_analytics (list): list of documents in dmps_send_to_wa_analytics collection
            db_general_message_details (list): list of documents in dmps_send_to_wa_analytics collection
            message_response_id (str) : unique id for the message or template sent
            broadcast_stats_doc (dict) : broadcast details documents in given time interval
            sent_sum : count of messages sent in given time interval
            delivered_sum : count of messages delivered in given time interval
            read_sum : count of messages read in given time interval
            failed_sum : count of messages failed in given time interval
            total_sum : count of messages  given time interval
            sent_count : count of messages sent in specific broadcast
            delivered_count : count of messages delivered in specific broadcast
            read_count : count of messages read in specific broadcast
            Returns:
            All above parameteres and variables
      
        """
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        obj_common = ClsCommon()
        self.client_number = str(client_number)
        db_client_waba_settings = obj_common.get_waba_settings_by_cc_client_number(self.client_number)
        if "error" in db_client_waba_settings or db_client_waba_settings == None:
            return jsonify({"error": "Invalid client number. Please enter appropriate client number"})    
        self.client_db_name = str(db_client_waba_settings["response"]["ew_id"]).lower() + "_" + db_client_waba_settings["response"]["waba_id"]
        self.client_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)
    

    def  func_broadcast_stats_invalid(self,int_broadcast_id,str_recipient_number):
        """Call this function when invalid contact"""
        try:
            lg.info("invalid number : " + str_recipient_number)
            self.client_db.broadcast_details_info.update_one({"broadcast_id": int_broadcast_id ,"recipient_number":str_recipient_number,"reason":"invalid"})
            self.client_db.broadcast_details.update_one({"broadcast_id": int_broadcast_id },{"$inc":{"recipient_failed_count":1}} )
            # self.client_db.broadcast_details.update_one({"broadcast_id": int_broadcast_id }, {"$push": {"failed_stats":{"recipient_number":str_recipient_number,"reason":"invalid"}},"$inc":{"recipient_failed_count":1} })
            # self.client_db.broadcast_details.update_one({"broadcast_id": int_broadcast_id}, {"$push": {"failed_stats":{"recipient_number":str(recipient_number),"error":"invalid"}},"$inc":{"recipient_failed_count": 1 } })
        except Exception as e:
            return jsonify({"error": {"id": "1181", "message": "Data Query Error"}, "success": False})

        return {"response":"message sent failed"}


    def func_broadcast_stats_valid(self,int_broadcast_id,str_broadcast_name,resp,str_client_number,str_recipient_number,var_message_body,varmess,passmess,failmess):
        """Call this function at the time of message sent in celery"""
        lg.info("whatsapp response : " + str(resp))
        # time.sleep(300)
        self.client_db.broadcast_details_info.insert_many(varmess)
        self.client_db.broadcast_details.update_one({"broadcast_id": int_broadcast_id },{"$inc": {"recipient_sent_count": passmess,"recipient_failed_count":failmess}})
        
        
        '''
        if "messages" in resp.keys():
            message_response_id = resp["messages"][0]["id"]
            # print(message_response_id)
            try:
                var_message_body = json.loads(var_message_body)
                #self.client_db.broadcast_details.update_one({"broadcast_id": int_broadcast_id }, {"$push": {"success_stats":{"recipient_number":str_recipient_number,"message_response_id":message_response_id,"sent":False,"delivered":False,"read":False,"button_response":"NA","dynamic_variables":var_message_body['template']['components']} }})
                self.client_db.broadcast_details.update_one({"broadcast_id": int_broadcast_id },{"$inc": {"recipient_sent_count": 1}})
                self.client_db.broadcast_details_info.update_one({"broadcast_id": int_broadcast_id ,"recipient_number":str_recipient_number,"message_response_id":message_response_id},{"$set":{"recipient_number":str_recipient_number,"message_response_id":message_response_id,"sent":True,"delivered":False,"read":False,"button_response":"NA","dynamic_variables":var_message_body['template']['components']} }, upsert=True)
            except Exception as e:
                error_data = "Error while adding analytics in DB: " + "recipient_number: " + str(str_recipient_number) + ", broadcast_id: " + str(int_broadcast_id) + str(e) 
                lg.critical(error_data)

                return jsonify({"error": {"id": "1180", "message": "Data Query Error"}, "success": False})

        else:
            try:
                lg.info(f"i came in else {var_message_body}")
                #self.client_db.broadcast_details.update_one({"broadcast_id": int_broadcast_id }, {"$push": {"sending_failed_stats":{"recipient_number":str_recipient_number, "reason": resp, "sent":False,"delivered":False,"read":False,"button_response":"NA","dynamic_variables":var_message_body['template']['components']}} })
                var_message_body = json.loads(var_message_body)
                lg.info(f"broadcast_id for wrong number  is {int_broadcast_id} and response is {var_message_body} and recipent number is {str_recipient_number}")
#                self.client_db.broadcast_details.update_one({"broadcast_id": int_broadcast_id, }, {"$push": {"failed_stats":{"recipient_number":str_recipient_number, "reason": "Invalid","dynamic_variables":var_message_body['template']['components'],"errors":resp['errors']}},"$inc":{"recipient_failed_count":1} })
                self.client_db.broadcast_details_info.update_one({"broadcast_id": int_broadcast_id, "recipient_number":str_recipient_number}, {"$set": {"recipient_number":str_recipient_number, "reason": "Invalid Number","dynamic_variables":"temp","errors":"temp"}},upsert=True)
                db_broadcast_details_info_update = self.client_db.broadcast_details_info.find_one({"broadcast_id": int_broadcast_id, "recipient_number":str_recipient_number})
                lg.info(f"db_details_info collection after updating failed reason {db_broadcast_details_info_update}")
                self.client_db.broadcast_details.update_one({"broadcast_id": int_broadcast_id },{"$inc":{"recipient_failed_count":1}} )
            except Exception as e:
                error_data = "Error while adding failed analytics in DB: " + "recipient_number: " + str(str_recipient_number) + ", broadcast_id: " + str(int_broadcast_id) + " | " + str(e) 
                lg.critical(error_data)
                return jsonify({"error": {"id": "1180", "message": "Data Query Error"}, "success": False})
     '''
        return {"response":"message sent"}


    @phone_access_required
    def func_broadcast_excel_download(self,broadcast_id,client_id,lg):
        """ Method to download the analytics of particular broadcast in excel"""
        try:
            """"
            #commented to add new logic
            # db_broadcast_details =  self.client_db.broadcast_details.find_one({"broadcast_id":broadcast_id},{"_id":0})
            db_broadcast_details =  self.client_db.broadcast_details_info.find_one({"broadcast_id":broadcast_id},{"_id":0})
            db_broadcast_details_li = []
            db_broadcast_details_updated = self.client_db.broadcast_details_info.find({"broadcast_id":broadcast_id},{"_id":0})
            for i in self.client_db.broadcast_details_info.find({"broadcast_id":broadcast_id},{"_id":0}):
                db_broadcast_details_li.append(i)
            db_broadcast_details_li = dict(enumerate(db_broadcast_details_li))
            success_stat = []
            failed_stat = []
            # reason = db_broadcast_details_li.get("reason","")
            for i in db_broadcast_details_updated:
                if 'reason' in i:
                    failed_stat.append(i)
                    lg.info(f"failed stat to get added in excel sheet is {failed_stat} ")
                else:
                    success_stat.append(i)
                    lg.info(f"success stat to get added in excel sheet is {success_stat} ")
            lg.info(f"Data to get added in excel sheet is {db_broadcast_details}")
            """
            # db_broadcast_details = self.client_db.broadcast_details_info.find_one({"broadcast_id": broadcast_id}, {"_id": 0})
            
            db_broadcast_details_li = list(self.client_db.broadcast_details_info.find({"broadcast_id": broadcast_id}, {"_id": 0,"data_delivered":0,"data_read":0}))
            lg.info(f"length of db_broadcast_details_li list is {db_broadcast_details_li} ")
            success_stat = [doc for doc in db_broadcast_details_li if 'reason' not in doc]
            failed_stat = [doc for doc in db_broadcast_details_li if 'reason' in doc]

            lg.info(f"Success stats to get added in excel sheet are {success_stat}")
            lg.info(f"Failed stats to get added in excel sheet are {failed_stat}")
            # lg.info(f"Data to get added in excel sheet is {db_broadcast_details}")

        except Exception as e:
            lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_details : " + str(e))    
            return jsonify({"id": "1222", "message": "Data Query Error", "description": "", "data": "", "success": False})
        
        if db_broadcast_details_li:
            # success_stat = db_broadcast_details["success_stats"]
            # success_stat = []
            # success_stat.append(db_broadcast_details)

            # failed_stat = db_broadcast_details["failed_stats"]
            # failed_stat = []
            for recipient in failed_stat:
                if recipient["reason"] == "Consent is False":
                    error_str = "Consent is False"
                else:
                    #error_str = "Invalid Number"
                    # lg.info(f"This is dict{recipient}")
                    # lg.info(f"Type of dict is {type(recipient)}")
                    if isinstance(recipient['reason'], dict):
                        # error_str = recipient.get("reason", {}).get("statuses", [{}])[0].get("errors", [{}])[0].get("title", "Invalid Number")
                         lg.info(f"insider")
                         error_str = (recipient.get("reason", {})
                             .get("entry", [{}])[0]
                             .get("changes", [{}])[0]
                             .get("value", {})
                             .get("statuses", [{}])[0]
                             .get("errors", [{}])[0]
                             .get("title", "Invalid Number"))
                    else:
                        error_str = recipient.get('reason','Invalid Number')

                    # error_str = recipient.get("reason", {}).get("statuses", [{}])[0].get("errors", [{}])[0].get("title","invalid number")
                success_stat.append({ "recipient_number": recipient["recipient_number"], "sent": "NA", "delivered": "NA", "read": "NA", "error": error_str})
            # lg.info(f"final list is {success_stat}")
            return jsonify({"id": "1223", "message": "Data fetched successfully", "description": "", "data": {"detailed_broadcast_stat": success_stat, "columns": ["recipient_number", "sent", "delivered", "read", "error", "button_response"]}, "success":True})
        else:
            return jsonify({"id": "1224", "message": "Data fetched successfully", "description": "", "data": {"detailed_broadcast_stat": "", "columns": ["recipient_number", "sent", "delivered", "read", "error", "button_response"]}, "success": False})



    @phone_access_required
    def func_broadcast_basic_analysis(self,client_number,client_id,all,start_time,end_time,email_id,page_size,page_num,str_template_name,all_template,lg):            
        """Fuction called to get analytics of broadcasts in given time range"""
        db_user_role = self.client_db.user_roles.find_one({"email_id": email_id}, {"_id": 0})
        
        if db_user_role == None:
            user_role = ""
        else: 
            user_role = db_user_role["role"]      
         
        if all == True and all_template == True: # all means all phone numbers
            try:
                if user_role == "DMP Admin":
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0})
                else:
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0})
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_details : " + str(e))    
                return jsonify({"id": "1212", "message": "Data Query Error", "description": "", "data": "", "success": False})
        
        elif all == True and all_template == False:
            try:
                if user_role == "DMP Admin":
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id,"template_name": str_template_name,"timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0})
                else:                    
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id,"template_name": str_template_name,"timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0})
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_details : " + str(e))    
                return jsonify({"id": "1212", "message": "Data Query Error", "description": "", "data": "", "success": False})        
        
        elif all == False and all_template == True:
            try:
                if user_role == "DMP Admin":
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id,"client_number": client_number,"timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0})
                else:
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id,"client_number": client_number,"timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0})
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_details : " + str(e))    
                return jsonify({"id": "1212", "message": "Data Query Error", "description": "", "data": "", "success": False})              
        
        else:
            try:
                if user_role == "DMP Admin":
                    broadcast_stats = self.client_db.broadcast_details.find({"client_number": client_number,"template_name": str_template_name, "timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0})
                else:       
                    broadcast_stats = self.client_db.broadcast_details.find({"client_number": client_number,"template_name": str_template_name, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0})
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_details : " + str(e))    
                return jsonify({"id": "1213", "message": "Data Query Error", "description": "", "data": "", "success": False})
                 
        broadcast_list = []      
        sent_sum =0
        delivered_sum = 0
        read_sum = 0
        failed_sum = 0
        total_sum = 0
        total_cost = 0
        list_broadcast_stat = (list(broadcast_stats)) 
        unique_templates = set(doc['template_name'] for doc in  list_broadcast_stat)
        unique_templates_length = len(unique_templates)
        lg.info(f"broadcast_stat list is {list_broadcast_stat}")
        for single_document in list_broadcast_stat:      
            sent_count =  single_document["recipient_sent_count"]
            sent_sum += sent_count
            delivered_count =single_document["recipient_delivered_count"]
            delivered_sum += delivered_count
            read_count = single_document["recipient_read_count"]
            read_sum += read_count
            failed_count = single_document["recipient_failed_count"] 
            failed_sum += failed_count
            total = single_document["recipient_total_count"]
            total_sum += total  
            # cost_total = single_document.get('cost',0)
            # lg.info(f"The cost is {cost_total}")
            # total_cost += cost_total 
            # lg.info(f"Total cost is {total_cost}")
        skips = page_size * (page_num - 1)       
        if all == True and all_template == True:
            try:
                if user_role == "DMP Admin":
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0}).skip(skips).limit(page_size).sort([("timestamp",-1)])    
                else:
                    #cursor = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size)
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size).sort([("timestamp",-1)])
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_details : " + str(e))    
                return jsonify({"id": "1214", "message": "Data Query Error", "description": "", "data": "", "success": False})              
        
        elif all == True and all_template == False:
            try:
                if user_role == "DMP Admin":
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id,"template_name": str_template_name, "timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0}).skip(skips).limit(page_size).sort([("timestamp",-1)])    
                else:
                    #cursor = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size)
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id,"template_name": str_template_name, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size).sort([("timestamp",-1)])
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_details : " + str(e))    
                return jsonify({"id": "1214", "message": "Data Query Error", "description": "", "data": "", "success": False})      
        
        elif all == False and all_template == True:
            try:
                if user_role == "DMP Admin":
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id,"client_number": client_number, "timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0}).skip(skips).limit(page_size).sort([("timestamp",-1)])
                else:
                    #cursor = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size)
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id,"client_number": client_number, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size).sort([("timestamp",-1)])
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_details : " + str(e))    
                return jsonify({"id": "1214", "message": "Data Query Error", "description": "", "data": "", "success": False})      
        
        else:
            try:
                if user_role == "DMP Admin":
                    cursor = self.client_db.broadcast_details.find({"client_number": client_number, "template_name": str_template_name,"timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0}).skip(skips).limit(page_size).sort([("timestamp",-1)])
                else:
                    #cursor = self.client_db.broadcast_details.find({"client_number": client_number, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size)
                    cursor = self.client_db.broadcast_details.find({"client_number": client_number, "template_name": str_template_name,"timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size).sort([("timestamp",-1)])
            except Exception as e:
                    lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_details : " + str(e))    
                    return jsonify({"id": "1215", "message": "Data Query Error", "description": "", "data": "", "success": False})        
        
        for document in cursor:
            doc = document
            lg.info(f"document is {doc}")
            prev_cost = 0.80 * document["recipient_total_count"]
            stats = {"template_name": document["template_name"], "broadcast_id": document["broadcast_id"], "broadcast_name": document["broadcast_name"], "sent": document["recipient_sent_count"], "delivered": document["recipient_delivered_count"], "read": document["recipient_read_count"], "failed": document["recipient_failed_count"], "total": document["recipient_total_count"], "timestamp": document["timestamp"],"category":document.get('category',""),"cost":document.get("cost",prev_cost)}            
            broadcast_list.append(stats)       
        broadcast_rows_sum = {"sent_sum": sent_sum, "delivered_sum": delivered_sum, "read_sum": read_sum, "failed_sum": failed_sum, "total_sum": total_sum,"cost_sum":total_sum*0.80,"template_lenth":unique_templates_length}
        count = len(list_broadcast_stat)
        return jsonify({"id": "1216", "message": "Data fetched successfully", "description": "", "data": {"broadcast_rows": broadcast_list, "broadcast_sum": broadcast_rows_sum, "count": count}, "success": True})

    


    @phone_access_required
    def broadcast_basic_billing_analytics(self,client_number,client_id,all,start_time,end_time,email_id,str_template_name,all_template,lg):            
        """Fuction called to get billing analytics of broadcasts in given time range"""
        db_user_role = self.client_db.user_roles.find_one({"email_id": email_id}, {"_id": 0})
        
        if db_user_role == None:
            user_role = ""
        else: 
            user_role = db_user_role["role"]      
         
        if all == True and all_template == True: # all means all phone numbers
            try:
                if user_role == "DMP Admin":
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0})
                else:
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0})
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_billing_details : " + str(e))    
                return jsonify({"id": "7012", "message": "Data Query Error", "description": "", "data": "", "success": False})
        
        elif all == True and all_template == False:
            try:
                if user_role == "DMP Admin":
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id,"template_name": str_template_name,"timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0})
                else:                    
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id,"template_name": str_template_name,"timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0})
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_billing_details : " + str(e))    
                return jsonify({"id": "7012", "message": "Data Query Error", "description": "", "data": "", "success": False})        
        
        elif all == False and all_template == True:
            try:
                if user_role == "DMP Admin":
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id,"client_number": client_number,"timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0})
                else:
                    broadcast_stats = self.client_db.broadcast_details.find({"ew_id": client_id,"client_number": client_number,"timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0})
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_billing_details : " + str(e))    
                return jsonify({"id": "7012", "message": "Data Query Error", "description": "", "data": "", "success": False})              
        
        else:
            try:
                if user_role == "DMP Admin":
                    broadcast_stats = self.client_db.broadcast_details.find({"client_number": client_number,"template_name": str_template_name, "timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0})
                else:       
                    broadcast_stats = self.client_db.broadcast_details.find({"client_number": client_number,"template_name": str_template_name, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0})
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_billing_details : " + str(e))    
                return jsonify({"id": "7012", "message": "Data Query Error", "description": "", "data": "", "success": False})
                 
        broadcast_list = []      
        sent_sum =0
        delivered_sum = 0
        read_sum = 0
        failed_sum = 0
        total_sum = 0
        total_cost = 0
        list_broadcast_stat = (list(broadcast_stats)) 
        unique_templates = set(doc['template_name'] for doc in  list_broadcast_stat)
        unique_templates_length = len(unique_templates)
        lg.info(f"broadcast_stat list is {list_broadcast_stat}")
        for single_document in list_broadcast_stat:      
            sent_count =  single_document["recipient_sent_count"]
            sent_sum += sent_count
            delivered_count =single_document["recipient_delivered_count"]
            delivered_sum += delivered_count
            read_count = single_document["recipient_read_count"]
            read_sum += read_count
            failed_count = single_document["recipient_failed_count"] 
            failed_sum += failed_count
            total = single_document["recipient_total_count"]
            total_sum += total  
            # cost_total = single_document.get('cost',0)
            # lg.info(f"The cost is {cost_total}")
            # total_cost += cost_total 
            # lg.info(f"Total cost is {total_cost}")     
        if all == True and all_template == True:
            try:
                if user_role == "DMP Admin":
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0}).sort([("timestamp",-1)])    
                else:
                    #cursor = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size)
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).sort([("timestamp",-1)])
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_billing_details : " + str(e))    
                return jsonify({"id": "7014", "message": "Data Query Error", "description": "", "data": "", "success": False})              
        
        elif all == True and all_template == False:
            try:
                if user_role == "DMP Admin":
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id,"template_name": str_template_name, "timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0}).sort([("timestamp",-1)])    
                else:
                    #cursor = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size)
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id,"template_name": str_template_name, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).sort([("timestamp",-1)])
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_billing_details : " + str(e))    
                return jsonify({"id": "7014", "message": "Data Query Error", "description": "", "data": "", "success": False})      
        
        elif all == False and all_template == True:
            try:
                if user_role == "DMP Admin":
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id,"client_number": client_number, "timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0}).sort([("timestamp",-1)])
                else:
                    #cursor = self.client_db.broadcast_details.find({"ew_id": client_id, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size)
                    cursor = self.client_db.broadcast_details.find({"ew_id": client_id,"client_number": client_number, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).sort([("timestamp",-1)])
            except Exception as e:
                lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_billing_details : " + str(e))    
                return jsonify({"id": "7014", "message": "Data Query Error", "description": "", "data": "", "success": False})      
        
        else:
            try:
                if user_role == "DMP Admin":
                    cursor = self.client_db.broadcast_details.find({"client_number": client_number, "template_name": str_template_name,"timestamp": {"$gte": start_time, "$lt": end_time}}, {"_id": 0}).sort([("timestamp",-1)])
                else:
                    #cursor = self.client_db.broadcast_details.find({"client_number": client_number, "timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).skip(skips).limit(page_size)
                    cursor = self.client_db.broadcast_details.find({"client_number": client_number, "template_name": str_template_name,"timestamp": {"$gte": start_time, "$lt": end_time}, "created_by": email_id}, {"_id": 0}).sort([("timestamp",-1)])
            except Exception as e:
                    lg.critical("ew_id=" + str(client_id) + " | " + "DB error - broadcast_details : " + str(e))    
                    return jsonify({"id": "7015", "message": "Data Query Error", "description": "", "data": "", "success": False})        
        
        for document in cursor:
            doc = document
            lg.info(f"document is {doc}")
            prev_cost = 0.80 * document["recipient_total_count"]
            stats = {"template_name": document["template_name"], "broadcast_id": document["broadcast_id"], "broadcast_name": document["broadcast_name"], "timestamp": document["timestamp"],"category":document.get('category',""),"cost":document.get("cost",prev_cost),"recipients":document.get("recipient_total_count")}            
            broadcast_list.append(stats) 
        count = len(list_broadcast_stat)     
        broadcast_rows_sum = {"cost_sum":total_sum*0.80,"template_lenth":unique_templates_length,"total_recipients":total_sum,"count":count}

        return jsonify({"id": "7016", "message": "Data fetched successfully", "description": "", "data": {"broadcast_rows": broadcast_list, "broadcast_sum": broadcast_rows_sum}, "success": True})

    @phone_access_required
    def func_send_conversational_analytics(self,int_from_timestamp,int_to_timestamp,lg):
            """Call this function to fetch analytics of templates sent from dmp"""           
            try:
                db_list_counts = list(self.client_db.converstaional_data_collection.find({"client_number":self.client_number, "timestamp": {"$gte": str(int_from_timestamp), "$lte": str(int_to_timestamp)}},{"_id": 0,"message_response_id":0,"int_id":0,"clientid":0,"client_number":0})) 
                # documents_with_cost = [[{**message, "cost": 0.8} for message in document]for document in db_list_counts]
                lg.info(f"length of Analytics list is {len(db_list_counts)}")
                mobile_count= list(self.client_db.converstaional_data_collection.find({"client_number":self.client_number, "timestamp": {"$gte": str(int_from_timestamp), "$lte": str(int_to_timestamp)}},{"user_mobile_number": 1,"_id": 0}))
                # no_of_users=len(set(mobile_count))
                unique_users=set(item['user_mobile_number'] for item in mobile_count)
                count = len(unique_users)

                
            except Exception as e:
                lg.critical("DB error - conversational_billing_analytics : " + str(e))
                return jsonify({"id": "8002", "msg": "Data Query Error", "description": "", "data": "", "success": False})
            if db_list_counts:
                total_conversational = len(db_list_counts)
                total = {}
                total.update({"total_count" :total_conversational})
                total.update({"total_cost":0.80*total["total_count"]})
                total.update({"no_of_users":count})
                return {"id": "8003", "message": "Data fetched successfully", "description": "", "data":db_list_counts,"total_count":total, "success":True}
            else:
                return {"id": "8003", "message": "Data fetched successfully", "description": "", "data":" ","total_count":"NO data found", "success":True}
