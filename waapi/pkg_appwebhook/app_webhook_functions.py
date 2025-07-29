# from flask.json import jsonify
# import requests
# import json
# from datetime import datetime
# from requests.auth import HTTPBasicAuth
# import logging.config
# import logging
# import yaml
# import os
# from global_variable import ew_db

# with open(os.path.dirname(__file__) + '/../conf/logging.yaml', 'r') as f:
#     config = yaml.safe_load(f.read())
#     logging.config.dictConfig(config)
# LOG = logging.getLogger('appWebhookLog')

# class AppWebhookData():

#     def save_webhook_data(self,data):
#         try:
#             if "entry" in data:
#                 if "changes" in data["entry"][0]:
#                     ew_id = ew_db.client_waba_settings.find_one({"waba_id":data["entry"][0]["id"]},{"ew_id":1,"_id":0})
#                     if "value" in data["entry"][0]["changes"][0]: 
#                         data_path = data["entry"][0]["changes"][0]
#                         # ew_db.webhook_data.insert_one({"ew_id":ew_id["ew_id"],"field":data_path["field"], "status":data_path["value"]["event"], "message_template_id":data_path["value"][ "message_template_id"], "message_template_language":data_path["value"][ "message_template_language"], "message_template_name":data_path["value"][ "message_template_name"], "reason":data_path["value"][ "reason"], "waba_id":data["entry"][0]["id"],"timestamp":data["entry"][0]["time"],"object":data["object"] })
#                         ew_db.webhook_data.insert_one({"ew_id":ew_id["ew_id"],"field":data_path["field"],"value":data_path["value"],"waba_id":data["entry"][0]["id"],"timestamp":data["entry"][0]["time"],"object":data["object"]})                 
#                         ew_db.template_mapping.update_one({"wa_response_template_id":str(data_path["value"][ "message_template_id"])}, {"$set":{"template_status":data_path["value"]["event"],"reason":data_path["value"][ "reason"]}})
#             return True
#         except:
#             LOG.error("Failed to save App Webhook Data")

#     def update_quality_displayname_tier(self,data):
#             try:            
#                 if "entry" in data:
#                     if "changes" in data["entry"][0]:
#                         ew_id = ew_db.client_waba_settings.find_one({"waba_id":data["entry"][0]["id"]},{"ew_id":1,"_id":0})                   
#                         if "value" in data["entry"][0]["changes"][0]: 
#                             if data["entry"][0]["changes"][0]["field"]== "phone_number_quality_update":
#                                 display_phone_number = data["entry"][0]["changes"][0]["value"]["display_phone_number"]
#                                 quality_from_webhook = data["entry"][0]["changes"][0]["value"]["event"]
#                                 quality_from_db = ew_db.client_profile.find_one({"client_id":ew_id["ew_id"], "client_number":display_phone_number},{"_id":0,"quality_rating":1})["quality_rating"]
#                                 if quality_from_webhook == "DOWNGRADE":   
#                                     if quality_from_db == "GREEN":
#                                         quality = "YELLOW"
#                                     elif quality_from_db == "YELLOW":
#                                         quality = "RED"
#                                     elif quality_from_db == "RED":
#                                         quality = "RED"                                 
#                                 elif quality_from_webhook == "UPGRADE":   
#                                     if quality_from_db == "GREEN":
#                                         quality = "GREEN"
#                                     elif quality_from_db == "YELLOW":
#                                         quality = "GREEN"
#                                     elif quality_from_db == "RED":
#                                         quality = "YELLOW"
#                                 else:
#                                     quality = quality_from_webhook


#                                 tier =  data["entry"][0]["changes"][0]["value"]["current_limit"]
                                
#                                 ew_db.client_profile.update_one({"client_id":ew_id["ew_id"], "client_number":display_phone_number},{"$set":{"quality_rating":quality,"message_tier":tier}})
                            
#                             if data["entry"][0]["changes"][0]["field"]== "phone_number_name_update": 
#                                 if data["entry"][0]["changes"][0]["value"]["decision"] == "APPROVED":                                
#                                     verified_name = data["entry"][0]["changes"][0]["value"]["requested_verified_name"]
#                                     display_phone_number = data["entry"][0]["changes"][0]["value"]["display_phone_number"]
#                                     ew_db.client_profile.update_one({"client_id":ew_id["ew_id"], "client_number":display_phone_number},{"$set":{"verified_name":verified_name}})
#                                     ew_db.client_business_info.find_one_and_update({"wa_phone_numbers.wa_number": display_phone_number[2:]},{"$set": {"wa_phone_numbers.$.wa_display_name":verified_name}})
#                 return True
#             except:
#                 LOG.error("Failed to save App Webhook Data")
                
#     def send_text_message(self,data):
#         try:
#             waba_data = ew_db.client_waba_settings.find_one({"ew_id":"EW5", "client_number":"8317269145"})
#             url = waba_data["url"]+"messages/"

#             ew_id = ew_db.client_waba_settings.find_one({"waba_id":data["entry"][0]["id"]},{"ew_id":1,"_id":0})
#             name = ew_db.client_profile.find_one({"client_id":ew_id["ew_id"]},{"verified_name":1,"_id":0})

#             data_path = data["entry"][0]["changes"][0]
#             if data_path["value"]["event"] == "REJECTED":
#                 send_data = {"client_name":name["verified_name"],"waba_id":data["entry"][0]["id"], "template_name":data_path["value"][ "message_template_name"],"status":data_path["value"]["event"],"object":data["object"],"reason":data_path["value"][ "reason"]}
#             else:
#                 send_data = {"client_name":name["verified_name"],"waba_id":data["entry"][0]["id"], "template_name":data_path["value"][ "message_template_name"],"status":data_path["value"]["event"],"object":data["object"]}

#             token = AppWebhookData().get_access_token(waba_data) 
#             contacts = ["919405779777","918956576028", "918983189241"]

#             for i in contacts:
#                 payload = json.dumps({"to":str(i),"type": "text","recipient_type": "individual","text": {"body": str(send_data)}})
#                 headers = {'Content-Type': 'application/json','Authorization': 'Bearer '+ token}
#                 response = requests.request("POST", url, headers=headers, data=payload, verify=False)
        
#             return "message sent!!"
#         except:
#             LOG.error("Failed to send Template Statuses")
    
#     def get_access_token(self,client):
#         token1 = client['access_token']
#         expiry = client['token_expires_after']
#         datetime_object = datetime.strptime(expiry[:-6], '%Y-%m-%d %H:%M:%S')
#         current_datetime = datetime.utcnow()
#         if datetime_object > current_datetime:
#             token = token1
#             pass
#         else:
#             url = client['url'] + 'users/login'
#             payload={}
#             headers = {'Content-Type': 'application/json'}
#             response = requests.request("POST", url, headers=headers, data=payload, auth=HTTPBasicAuth(client['username'], client['password']),verify=False)
#             users = response.json()
#             for user in users['users']:
#                 token = user['token']
#                 expiry = user['expires_after']
#                 clients = ew_db.client_waba_settings.update_one({"ew_id": client['ew_id']},{"$set": {"access_token": token, "token_expires_after": expiry}})
#         return token