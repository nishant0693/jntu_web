# """
#     * Copyright (C) engagely.ai - All Rights Reserved 
#     * About File: Contains schemas of DMP webhook package only
#     * Description: All the DMP webhook functions present here
# """

# import yaml
# import logging
# import logging.config
# import os.path
# from flask import jsonify

# # Import custom packages
# from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit


# # with open(os.path.dirname(__file__) + '/../conf/logging.yaml', 'r') as f:
# #     config = yaml.safe_load(f.read())
# #     logging.config.dictConfig(config)
# # LOG = logging.getLogger('webhookLog')


# class ClsDmpWebhook:
#     """ Class for DMP webhook functions """

#     def __init__(self):
#         """ Create or initialize object and variables """
#         pass

#     def func_webhook_dmp(self,data):
#         """ Function for DMP webhook """
#         data_keys = list(data.keys())

#         if (data_keys[0] == 'statuses'):
#             str_message_id = data['statuses'][0]['id']
#             str_status =  data['statuses'][0]['status']
#             str_conversation_id = data["statuses"][0]["conversation"]["id"]
#             str_billable = data["statuses"][0]["pricing"]["billable"]
#             str_conversation_category = data["statuses"][0]["pricing"]["category"]
        
#             if str_status == "sent":
#                 try:
#                     ew_db = ClsMongoDBInit.get_db_client()
#                     doc = ew_db.dmps_send_to_wa_analytics.find_one_and_update({"statuses.message_response_id": str_message_id},{"$set": {"statuses.$.sent":True}})
#                     if doc == None:
#                         ew_db.general_messages_details.find_one_and_update({"messages_detail.message_id": str_message_id},{"$set": {"messages_detail.$.sent":True,"messages_detail.$.conversation_id":str_conversation_id,"messages_detail.$.billable":str_billable,"messages_detail.$.conversation_type":str_conversation_category}})        
#                 except Exception as e:
#                     return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
#                 finally:
#                     ew_db.client.close()
                
#                 if doc is not None:
#                     sent_count = doc["recipient_sent_count"] +1
#                     sender_contact =doc["sender_number"]
#                     timestamp = doc["timestamp"]
#                     template_name = doc["template_name"]
#                     try:
#                         ew_db = ClsMongoDBInit.get_db_client()
#                         ew_db.dmps_send_to_wa_analytics.update_one({"sender_number":sender_contact,"timestamp":timestamp,"template_name":template_name},{"$set":{"recipient_sent_count":sent_count}})
#                     except Exception as e:
#                         return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
#                     finally:
#                         ew_db.client.close()
            
#             if str_status == "delivered":
#                 try:
#                     ew_db = ClsMongoDBInit.get_db_client()
#                     doc = ew_db.dmps_send_to_wa_analytics.find_one_and_update({"statuses.message_response_id": str_message_id},{"$set": {"statuses.$.delivered":True}})
#                     if doc == None:
#                         ew_db.general_messages_details.find_one_and_update({"messages_detail.message_id": str_message_id},{"$set": {"messages_detail.$.delivered":True,"messages_detail.$.conversation_id":str_conversation_id,"messages_detail.$.billable":str_billable,"messages_detail.$.conversation_type":str_conversation_category}})
#                 except Exception as e:
#                     return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
#                 finally:
#                     ew_db.client.close()

#                 if doc is not None:
#                     sender_contact =doc["sender_number"]
#                     timestamp = doc ["timestamp"]
#                     template_name = doc["template_name"]
#                     delivered_count = doc["recipient_delivered_count"] +1
#                     try:
#                         ew_db = ClsMongoDBInit.get_db_client()
#                         ew_db.dmps_send_to_wa_analytics.update_one({"sender_number":sender_contact,"timestamp":timestamp,"template_name":template_name},{"$set":{"recipient_delivered_count":delivered_count}})
#                     except Exception as e:
#                         return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
#                     finally:
#                         ew_db.client.close()                
            
#             if str_status == "read":
#                 try:
#                     ew_db = ClsMongoDBInit.get_db_client()
#                     doc = ew_db.dmps_send_to_wa_analytics.find_one_and_update({"statuses.message_response_id": str_message_id},{"$set":{"statuses.$.read":True}})
#                     if doc == None:
#                         ew_db.general_messages_details.find_one_and_update({"messages_detail.message_id": str_message_id},{"$set": {"messages_detail.$.read":True,"messages_detail.$.delivered":True,"messages_detail.$.conversation_id":str_conversation_id,"messages_detail.$.billable":str_billable,"messages_detail.$.conversation_type":str_conversation_category}})
#                 except Exception as e:
#                     return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
#                 finally:
#                     ew_db.client.close()                       
                
#                 if doc is not None:    
#                     read_count = doc["recipient_read_count"] +1
#                     sender_contact =doc["sender_number"]
#                     timestamp = doc["timestamp"]
#                     template_name = doc["template_name"]
#                     try:
#                         ew_db = ClsMongoDBInit.get_db_client()
#                         ew_db.dmps_send_to_wa_analytics.update_one({"sender_number":sender_contact,"timestamp":timestamp,"template_name":template_name},{"$set":{"recipient_read_count":read_count}})
#                     except Exception as e:
#                         return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
#                     finally:
#                         ew_db.client.close()
                    
#                     element = doc["statuses"]
#                     for recipient in element:
#                         if recipient["message_response_id"] == str_message_id:
#                             if recipient["delivered"]== False:
#                                 sender_contact =doc["sender_number"]
#                                 timestamp = doc ["timestamp"]
#                                 template_name = doc["template_name"]
#                                 delivered_count = doc["recipient_delivered_count"] +1
#                                 try:
#                                     ew_db = ClsMongoDBInit.get_db_client()
#                                     ew_db.dmps_send_to_wa_analytics.update_one({"sender_number":sender_contact,"timestamp":timestamp,"template_name":template_name},{"$set":{"recipient_delivered_count":read_count}})                                
#                                     ew_db.dmps_send_to_wa_analytics.find_one_and_update({"statuses.message_response_id": str_message_id},{"$set": {"statuses.$.delivered":True}})
#                                 except Exception as e:
#                                     return jsonify({"error": {"id": "5005", "message": "Data Query Error"}, "success": False})
#                                 finally:
#                                     ew_db.client.close() 
        
#         return "200"