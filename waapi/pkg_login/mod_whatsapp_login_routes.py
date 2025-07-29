
from flask_cors import cross_origin
from flask import Flask, jsonify, request, Blueprint

# Import custom packages
from wacore.auth.mod_login_functions import ClsWhatsappLogin

app_login = Blueprint('app_login', __name__,url_prefix='/waapi/login')


class ClsLogin():

    def __init__(self):
        pass

    # @app_login.route("/login", methods=["POST"])
    # @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    # def login():
    #     """ Method called to get login into system """
    #     data = request.json
    #     obj_login = ClsWhatsappLogin()
    #     result = obj_login.get_login(data)
    #     return result


    @app_login.route("/verify_dmp_login", methods=["POST"])
    @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    ##@jwt_refresh_token_required
    def verify_dmp_login():
        """ Method called to get login into system """
        data = request.json
        auth_header = str(data["token"])
        auth_header = auth_header.replace("____",".")
        obj_login = ClsWhatsappLogin()
        decoded_token = obj_login.func_dmp_token_decode(data)
        print(f"decoded_token is {decoded_token}")
        cur_user = decoded_token['identity']
        print(f" current user is {cur_user}")
        decrypted_token = obj_login.func_dmp_token_decrypt(cur_user)
        # if "BotId" in decoded_token["identity"].keys() and "userName" in decoded_token["identity"].keys():
        #     bot_id = decoded_token["identity"]["BotId"]
        #     email_id = decoded_token["identity"]["userName"]
            
        #     result = obj_login.func_verify_dmp_login(bot_id,email_id)            
        #     return result
        if "BotId" in decrypted_token and "userName" in decrypted_token:
                bot_id = decrypted_token["BotId"]
                print(f"thisisbot_id{bot_id}")
                email_id = decrypted_token["userName"]
                print(f"thiseial_id{email_id}")
            
                result = obj_login.func_verify_dmp_login(bot_id,email_id,auth_header)            
                return result
        else:
            return jsonify({"id": "xxxx", "message": "Invalid data", "description": "Invalid input token", "data": "", "success": False})


    # @app_login.route("/get_jwt", methods=["POST"])
    # @cross_origin(origin='*',headers=['Content-Type','Authorization'])
    # def get_jwt():
    #     data = request.json
    #     obj_login = ClsWhatsappLogin()
    #     result = obj_login.login_func(data["ew_id"],data["client_number"],data["email_id"],data["bot_id"])
    #     return result