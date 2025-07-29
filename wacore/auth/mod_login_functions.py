import email
from http import client
from logging import exception
import jwt
import base64
from flask_jwt_extended import get_jwt, create_access_token, verify_jwt_in_request
from flask_bcrypt import Bcrypt
from datetime import datetime, timedelta
from flask import jsonify, request
from functools import wraps

import json
from pyaes256 import PyAES256
aes_obj = PyAES256()

# Import custom packages
from ..pkg_extras.mod_common import ClsMongoDBInit, ClsCommon

bcrypt = Bcrypt()


class ClsWhatsappLogin():
    """ Class called to perform login function for whatsapp DMP"""
    
    def __init__(self):
        """ Initialize required object and variables """
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        self.error = ()
        pass


    def func_verify_dmp_login(self,bot_id,email_id,auth_header):
        """ Method called for vrify DMP login"""
        print("user details",{"bot_id": bot_id, "email_id": email_id})
        try:
            db_client_waba_settings = self.ew_db.client_waba_settings.find_one({"bot_id": bot_id},{"_id":0, "ew_id":1, "cc_client_number": 1,"access_token":1})
        except Exception as e:
            return jsonify({"id": "5005", "message": "Data Query Error", "description": "", "data": "", "success": False})
        if db_client_waba_settings == None:
            return jsonify({"id": "xxxx", "message": "Invalid user input", "description": "Bot id not linked with this whatsapp interface", "data": "", "success": False})
        else:
            token_doc = {"Token":auth_header}
            print(f"thisclientwabasettingsqueryresult{db_client_waba_settings}")
            # print("dev")
            # return jsonify({"id": "xxxx", "message": "Data fetched successfully", "description": "", "data": {"ew_id": db_client_waba_settings["ew_id"], "bot_id": bot_id, "token": db_client_waba_settings["access_token"]}, "success": True})
            new_token = self.login_func(db_client_waba_settings["ew_id"], db_client_waba_settings["cc_client_number"], email_id, bot_id,token_doc)
            
            return jsonify({"id": "xxxx", "message": "Data fetched successfully", "description": "", "data": {"ew_id": db_client_waba_settings["ew_id"], "bot_id": bot_id, "token": new_token.json["data"]["access_token"]}, "success": True})


    def func_dmp_token_decode(self,data):
        """ Method called to decode DMP token """
        token = str(data["token"])
        token = token.replace("____",".")
        token = jwt.decode(token,options={"verify_signature": False})
        return token
    
    def func_dmp_token_decrypt(self,decoded_token):
        """ Method called to decrypt DMP token"""
        encrypted_token_data = {"url": decoded_token["token1"], "salt": decoded_token["token2"].encode(), "iv": decoded_token["token3"].encode()} 
        decrypted_token = aes_obj.decrypt(url=decoded_token["token1"], salt=decoded_token["token2"].encode(), iv=decoded_token["token3"].encode(), password="98bbb23232323hfhfhfhfbbb")
        decrypted_data = json.loads(decrypted_token)
        return decrypted_data



    def login_func(self,ew_id,client_number,email_id,bot_id,token_doc):
        message = ""
        res_data = {}
        status = False
        
        user = email_id
        
        try:
            # db_client_waba_settings = ClsCommon().get_waba_settings_by_bot_id(bot_id) 
            db_client_waba_settings=self.ew_db.client_waba_settings.find_one({"bot_id": bot_id},{"_id":0, "ew_id":1, "cc_client_number": 1,"access_token":1,"waba_id":1})
            print(f"thisisdb_client_waba_settingsinsidelogin_func{db_client_waba_settings}")
        except:
            return jsonify({"id": "1501", "message": "Invalid credentials", "description": "Invalid bot id. Bot id is not linked with internal DB", "data": "", "success": False})        
        if "error" in db_client_waba_settings:
            return jsonify({"id": "1501", "message": "Invalid credentials", "description": "Invalid bot id. Bot id is not linked with internal DB", "data": "", "success": False})    
            
        try:
            client_db_name = db_client_waba_settings["ew_id"].lower() + "_" + db_client_waba_settings["waba_id"]
            cl_db = ClsMongoDBInit.get_cl_db_client(client_db_name)
            print(f"thisiscl_db{cl_db}")
        except:
            return jsonify({"id": "1501", "message": "DB error", "description": "DB error", "data": "", "success": False})
            
        db_user_roles = cl_db.user_roles.find_one({"email_id": email_id}, {"id":0})
        print(f"thisisdb_user_roles{db_user_roles}")
        if db_user_roles == None:
            return jsonify({"id": "1501", "message": "Invalid credentials", "description": "Invalid email id or bot id", "data": "", "success": False})

        if user and db_user_roles != None:
            expire_time = datetime.utcnow().now() + timedelta(minutes=60)
            additional_claim_info = {"email_id": email_id, "bot_id": bot_id, "ew_id": ew_id, "accessible_phones": db_user_roles["accessible_phones"]}
            access_token = create_access_token(email_id, additional_claims=additional_claim_info)
            print("thisisaccess_token{access_token}")
            message = f"User authenticated"
            code = 200
            status = True
            res_data['access_token'] = access_token
            # print(f"thisisres_data{res_data}")
            res_data['user'] = user
            res_data['token_expire_after'] = str(expire_time)
            print(f"thisisres_data{res_data}")
            cl_db.dmp_token_collection.insert_one(token_doc)
        else:
            message = "invalid login details"
            code = 401
            status = False

        return jsonify({"id": "5100", "status": status, "data": res_data, "message":message, "description": "", "success": True})



class ClsClientApiLogin():
    def __init__(self):
        """ Initialize required object and variables """
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        self.error = ()
        pass


    def login_func(self,data):
        message = ""
        res_data = {}
        code = 500
        status = False
        
        user = self.ew_db.client_api_users.find_one({"email": f'{data["email"]}'})

        if user:
            user['_id'] = str(user['_id'])

            if user and bcrypt.check_password_hash(user['password'], data['password']):
                expire_time = datetime.utcnow().now() + timedelta(minutes=60)
                additional_claim_info = {"bot_id": user["bot_id"]}
                access_token = create_access_token(data["email"],additional_claims=additional_claim_info)

                del user['password']
                del user['bot_id']
                del user['created']
                del user['_id']
                del user['username']

                message = f"User authenticated"
                code = 200
                status = True
                res_data['access_token'] = access_token
                res_data['user'] = user
                res_data['token_expire_after'] = str(expire_time)

            else:
                message = "wrong password"
                code = 401
                status = False
        else:
            message = "invalid login details"
            code = 401
            status = False

        return jsonify({"id": "2010", "status": status, "data": res_data, "message":message, "description": "", "success": True})



class ClsConcentClientApiLogin():
    def __init__(self):
        """ Initialize required object and variables """
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        self.error = ()
        pass


    def login_func(self,data):
        message = ""
        res_data = {}
        code = 500
        status = False
        
        user = self.ew_db.client_api_users.find_one({"email": f'{data["email"]}'})

        if user:
            user['_id'] = str(user['_id'])

            if user and bcrypt.check_password_hash(user['password'], data['password']):
                expire_time = datetime.utcnow().now() + timedelta(minutes=60)
                additional_claim_info = {"bot_id": user["bot_id"]}
                access_token = create_access_token(data["email"],additional_claims=additional_claim_info)

                del user['password']
                del user['bot_id']
                del user['created']
                del user['_id']
                del user['username']

                message = f"User authenticated"
                code = 200
                status = True
                res_data['access_token'] = access_token
                res_data['user'] = user
                res_data['token_expire_after'] = str(expire_time)

            else:
                message = "wrong password"
                code = 401
                status = False
        else:
            message = "invalid login details"
            code = 401
            status = False

        return jsonify({"id": "2010", "status": status, "data": res_data, "message":message, "description": "", "success": True})



class ClsDmpLogin():
    def __init__(self):
        """ Initialize required object and variables """
        self.ew_db = ClsMongoDBInit.get_ew_db_client()
        self.error = ()


    def login_func(self,bot_id,data):
        message = ""
        res_data = {}
        code = 500
        status = False
        
        user = self.ew_db.dmp_users.find_one({"email": f'{data["email"]}'})
        if user:
            user['_id'] = str(user['_id'])

            if user and bcrypt.check_password_hash(user['password'], data['password']):
                expire_time = datetime.utcnow().now() + timedelta(minutes=60)
                additional_claim_info = {"bot_id": bot_id}
                access_token = create_access_token(data["email"],additional_claims=additional_claim_info)

                del user['password']
                del user['created']
                del user['_id']
                del user['username']

                message = f"User authenticated"
                code = 200
                status = True
                res_data['access_token'] = access_token
                res_data['user'] = user
                res_data['token_expire_after'] = str(expire_time)

            else:
                message = "wrong password"
                code = 401
                status = False
        else:
            message = "invalid login details"
            code = 401
            status = False

        return jsonify({"id": "2122", "status": status, "data": res_data, "message": message, "description": "", "success": True})


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "Authorization" in request.headers:
            token = request.headers["Authorization"]
            if not token:
                return jsonify({"id": "5100", "message": "Token is missing", "description": "", "data": "", "success": False})
            try:
                verify_jwt_in_request()
            except Exception as e:
                return jsonify({"id": "5100", "message": "Unauthorized access"+str(e), "description": "", "data": "", "success": False})
        
            return f(*args, **kwargs)
        else:
            return jsonify({"id": "5100", "message": "Invalid Authorization", "description": "", "data": "", "success": False})

    return decorated


def phone_access_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "Authorization" in request.headers:
            token = request.headers["Authorization"]
            if not token:
                return jsonify({"id": "5100", "message": "Token is missing", "description": "", "data": "", "success": False})
            claims = get_jwt()
            client_number = request.args.get("client_number")
            if client_number == None:
                try:
                    data = request.json
                    client_number = data["client_number"]
                except:
                    client_number = None
            if client_number != None:
                lst_phone_data = list(map(str, claims["accessible_phones"]))
                if client_number not in lst_phone_data:
                    return jsonify({"id": "1501", "message": "This phone number is not accessible for your user role", "description": "", "data": "", "success": False}) 
            
            return f(*args, **kwargs)
        else:
            return jsonify({"id": "5100", "message": "Invalid Authorization", "description": "", "data": "", "success": False})

    return decorated