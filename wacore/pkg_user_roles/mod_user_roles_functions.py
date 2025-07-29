import datetime
from flask import jsonify
# import custom packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit


class ClsUserRolesFunction():
    """ Class called to perform user roles actions """
    pass

    def __init__(self,db_client_waba_settings):
        """
        Initialize required object and variables:
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

        
    def func_add_user_role(self,data,lg):
        """ Method called to add user roles """
        str_email = data["email_id"]
        lst_accessible_phones = list(map(int,data["accessible_phones"]))
        created_timestamp = int(datetime.datetime.now().timestamp())
        str_role = data["role"]
        try:    
            db_user_roles = self.cl_db.user_roles.find_one({"email_id": str_email},{"_id":0})
            if db_user_roles == None:
                self.cl_db.user_roles.insert_one({"email_id": str_email, "accessible_phones": lst_accessible_phones, "created_timestamp": created_timestamp, "role": str_role})
            else:
                self.cl_db.user_roles.find_one_and_update({"email_id": str_email}, {"$set": {"accessible_phones": lst_accessible_phones}})
        except Exception as e:
            lg.critical("DB error - user roles : " + str(e))
            return jsonify({"id": "1274", "message": "Data Query Error", "description": "", "data": "", "success": False})
            
        return jsonify({"id": "1275", "message": "User added successfully", "description": "", "data": "", "success": True})


    def func_list_of_numbers(self,bot_id,lg):
        try:
            db_client_business_info = self.ew_db.client_business_info.find_one({"bot_id": bot_id}, {"_id":0})
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - client_business_info : " + str(e))    
            return jsonify({"id": "1282", "message": "Data Query Error", "description": "", "data": "", "success": False})

        if db_client_business_info == None or "wa_phone_numbers" not in db_client_business_info:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - client_business_info : None")    
            return jsonify({"id": "1283", "message": "Data Query Error", "description": "", "data": "", "success": False})
        try:
            lst_phone_numbers = []
            for number_data in db_client_business_info["wa_phone_numbers"]:
                cc_client_number = number_data["country_code"][1:] + number_data["wa_number"]
                lst_phone_numbers.append(cc_client_number)
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - Error in key response : " + str(e))    
            return jsonify({"id": "1284", "message": "Key response error", "description": "Error in key response", "data": "", "success": False})
        
        return jsonify({"id": "1285", "message": "Data fetched successfully", "description": "", "data": {"client_numbers": lst_phone_numbers}, "success": True})


    def func_get_user_role(self,data,lg):
        str_email = data["email_id"]
        try:    
            db_user_roles = self.cl_db.user_roles.find_one({"email_id": str_email},{"_id":0})

            if db_user_roles == None:
                lg.critical("DB error - user roles : None")
                return jsonify({"id": "1294", "message": "DB error", "description": "No user role is available for this email id", "data": "", "success": False})
            list_of_numbers = list(map(str, db_user_roles["accessible_phones"]))
        except Exception as e:
            lg.critical("DB error - user roles : " + str(e))
            return jsonify({"id": "1295", "message": "Data Query Error", "description": "", "data": "", "success": False})
        
        return jsonify({"id": "1296", "message": "User roles fetched successfully", "description": "", "data": {"email_id": data["email_id"], "list_of_accessible_phones": list_of_numbers}, "success": True})