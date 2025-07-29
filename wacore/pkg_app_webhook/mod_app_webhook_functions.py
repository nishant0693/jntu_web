from flask import jsonify 

# Import custom packages
from ..pkg_extras.mod_common import ClsCommon
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
# from ..global_variable import graph_url, url_for_dmp
from walogger.walogger import WaLogger


# Initialize logger with name that we want or any other
obj_log = WaLogger('pktmp')
lg = obj_log.get_logger()


class ClsAppWebhook():
    """ Class called to perform appWebhook operations """

    def __init__(self,waba_id):
        lg.critical("inside class dev")
        lg.info("inside class dev")

        """
        Initialize required object and variables.      
        """
        # Get clinet WABA settings 
        obj_common = ClsCommon()
        try:
            db_client_waba_settings = obj_common.get_waba_settings_by_waba_id(waba_id)
        except Exception as e:
            lg.critical("waba_id=" + str(db_client_waba_settings) + " | " + "DB error - Failed to fetch WABA information:" + str(e))    
            return jsonify({"id": "1171", "message": "Failed to fetch phone information. Please, try again.", "description": "Invalid client_number. Client number is not linked with internal DB", "data": "", "success": False})

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


    def fn_save_data(self,data):
        """ If event type not match Just Save It """
        try:
            # Save All WABA related history 
            self.cl_db.waba_history.insert_one(data)
            return jsonify({"id": "AW-01", "message": "App webhook data saved", "description": "None", "data": "None", "success": True})
        except:
            lg.error(f"EXCP:Failed save webhook data: {data}")
            return jsonify({"success": False})
        
    def fn_update_temp_status(self,data):
        """ If event type is Template_Status_Update """
        try:
            # For Saving All WABA related history 
            self.cl_db.waba_history.insert_one(data)
            if "entry" in data:
                if "changes" in data["entry"][0]:
                    ew_id = self.ew_db.client_waba_settings.find_one({"waba_id":data["entry"][0]["id"]},{"ew_id":1,"_id":0})
                    if "value" in data["entry"][0]["changes"][0]: 
                        data_path = data["entry"][0]["changes"][0]

                        # Update Tempalte Status
                        self.cl_db.template_mapping.update_one({"wa_response_template_id":str(data_path["value"][ "message_template_id"])}, {"$set":{"template_status":data_path["value"]["event"],"reason":str(data_path["value"][ "reason"])}})
            return jsonify({"id": "AW-02", "message": "Templates status updated", "description": "None", "data": "None", "success": True})
        except:
            lg.error("EXCP:Failed to update template status.")
            return jsonify({"success": False})

    def fn_update_phone_quality_tier(self,data):
            """ If event type is Phone_Quality_Updates with tier"""
            try:
                # For Saving All WABA related history 
                self.cl_db.waba_history.insert_one(data)
                if "entry" in data:
                    if "changes" in data["entry"][0]:
                        ew_id = self.ew_db.client_waba_settings.find_one({"waba_id":data["entry"][0]["id"]},{"ew_id":1,"_id":0})                   
                        if "value" in data["entry"][0]["changes"][0]: 
                            if data["entry"][0]["changes"][0]["field"]== "phone_number_quality_update":
                                display_phone_number = data["entry"][0]["changes"][0]["value"]["display_phone_number"]
                                quality_from_webhook = data["entry"][0]["changes"][0]["value"]["event"]
                                quality_from_db = self.ew_db.client_profile.find_one({"client_id":ew_id["ew_id"], "client_number":display_phone_number},{"_id":0,"quality_rating":1})["quality_rating"]
                                if quality_from_webhook == "DOWNGRADE":   
                                    if quality_from_db == "GREEN":
                                        quality = "YELLOW"
                                    elif quality_from_db == "YELLOW":
                                        quality = "RED"
                                    elif quality_from_db == "RED":
                                        quality = "RED"                                 
                                elif quality_from_webhook == "UPGRADE":
                                    if quality_from_db == "GREEN":
                                        quality = "GREEN"
                                    elif quality_from_db == "YELLOW":
                                        quality = "GREEN"
                                    elif quality_from_db == "RED":
                                        quality = "YELLOW"
                                elif quality_from_webhook == "FLAGGED":
                                    quality = "FLAGGED"
                                elif quality_from_webhook == "UNFLAGGED":
                                    quality = "GREEN"
                                else:
                                    quality = quality_from_webhook
                                tier =  data["entry"][0]["changes"][0]["value"]["current_limit"]
                                self.ew_db.client_profile.update_one({"client_id":ew_id["ew_id"], "client_number":display_phone_number},{"$set":{"quality_rating":quality,"message_tier":tier}})
                                lg.info("SAVED IN FN")
                            # # For phone name update make seperate function
                            # if data["entry"][0]["changes"][0]["field"]== "phone_number_name_update": 
                            #     if data["entry"][0]["changes"][0]["value"]["decision"] == "APPROVED":                                
                            #         verified_name = data["entry"][0]["changes"][0]["value"]["requested_verified_name"]
                            #         display_phone_number = data["entry"][0]["changes"][0]["value"]["display_phone_number"]
                            #         ew_db.client_profile.update_one({"client_id":ew_id["ew_id"], "client_number":display_phone_number},{"$set":{"verified_name":verified_name}})
                            #         ew_db.client_business_info.find_one_and_update({"wa_phone_numbers.wa_number": display_phone_number[2:]},{"$set": {"wa_phone_numbers.$.wa_display_name":verified_name}})
                return jsonify({"id": "AW-03", "message": "Phone quality updated", "description": "None", "data": "None", "success": True})
            except:
                lg.error("EXCP:Failed to update phone status.")
                return jsonify({"success": False})


    # def send_text_message(self,data):
    #     try:
    #         waba_data = ew_db.client_waba_settings.find_one({"ew_id":"EW5", "client_number":"8317269145"})
    #         url = waba_data["url"]+"messages/"

    #         ew_id = ew_db.client_waba_settings.find_one({"waba_id":data["entry"][0]["id"]},{"ew_id":1,"_id":0})
    #         name = ew_db.client_profile.find_one({"client_id":ew_id["ew_id"]},{"verified_name":1,"_id":0})

    #         data_path = data["entry"][0]["changes"][0]
    #         if data_path["value"]["event"] == "REJECTED":
    #             send_data = {"client_name":name["verified_name"],"waba_id":data["entry"][0]["id"], "template_name":data_path["value"][ "message_template_name"],"status":data_path["value"]["event"],"object":data["object"],"reason":data_path["value"][ "reason"]}
    #         else:
    #             send_data = {"client_name":name["verified_name"],"waba_id":data["entry"][0]["id"], "template_name":data_path["value"][ "message_template_name"],"status":data_path["value"]["event"],"object":data["object"]}

    #         token = AppWebhookData().get_access_token(waba_data) 
    #         contacts = ["919405779777","918956576028", "918983189241"]

    #         for i in contacts:
    #             payload = json.dumps({"to":str(i),"type": "text","recipient_type": "individual","text": {"body": str(send_data)}})
    #             headers = {'Content-Type': 'application/json','Authorization': 'Bearer '+ token}
    #             response = requests.request("POST", url, headers=headers, data=payload, verify=False)
        
    #         return "message sent!!"
    #     except:
    #         LOG.error("Failed to send Template Statuses")
    
    # def get_access_token(self,client):
    #     token1 = client['access_token']
    #     expiry = client['token_expires_after']
    #     datetime_object = datetime.strptime(expiry[:-6], '%Y-%m-%d %H:%M:%S')
    #     current_datetime = datetime.utcnow()
    #     if datetime_object > current_datetime:
    #         token = token1
    #         pass
    #     else:
    #         url = client['url'] + 'users/login'
    #         payload={}
    #         headers = {'Content-Type': 'application/json'}
    #         response = requests.request("POST", url, headers=headers, data=payload, auth=HTTPBasicAuth(client['username'], client['password']),verify=False)
    #         users = response.json()
    #         for user in users['users']:
    #             token = user['token']
    #             expiry = user['expires_after']
    #             clients = ew_db.client_waba_settings.update_one({"ew_id": client['ew_id']},{"$set": {"access_token": token, "token_expires_after": expiry}})
    #     return token
