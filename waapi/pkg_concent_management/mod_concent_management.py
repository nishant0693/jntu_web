from flask import request, Blueprint
from flask_jwt_extended import get_jwt

# Import custom package
from wacore.auth.mod_login_functions import ClsConcentClientApiLogin
from wacore.pkg_concent_management import mod_concent_management_functions
from wacore.auth.mod_login_functions import token_required
from walogger.walogger import WaLogger
from wacore.pkg_extras.mod_common import ClsCommon


app_concent_api = Blueprint('app_concent_api', __name__,url_prefix='/waapi/concent')

# Initialize logger with name that we want or any other
obj_log = WaLogger('pkconcentapi')
lg = obj_log.get_logger()


class ConcentClientLogin():
    
    def __init__(self):
        pass
    
    @app_concent_api.route('/login', methods=['POST'])
    def logs():
        """ Method called to get access token after login """
        data = request.json
        result =  ClsConcentClientApiLogin().login_func(data)
        return result

# for concent upload
# for customer details  - query api
class ConcentManagementAPI:
    def __init__(self):
        """ Create or initialize object and variables """
        pass
    
    @app_concent_api.route('/add_recipients', methods=['POST'])
    @token_required
    def add_recipients():
        claims = get_jwt()
        bot_id = claims["bot_id"]
        data = request.json
        lg.info(f"request for consent_api is {data}")
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:    
            lg.critical("bot_id =" + str(bot_id) + " | " + "DB error - client_waba_settings : " + str(e))    
            return jsonify({"id": "1092", "message": "Data Query Error", "description": "", "data": "", "success": False})

        obj = mod_concent_management_functions.ClsConcentManagementFunc(db_client_waba_settings)
        response = obj.func_add_recipients(data)
        return response
    

    @app_concent_api.route('/recepient_details', methods=['POST'])
    @token_required
    def get_recepient_details():
        claims = get_jwt()
        data = request.json
        bot_id = claims["bot_id"]
        data = request.json
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("bot_id=" + str(bot_id) + " | " + "DB error - client_waba_settings : " + str(e))    
            return jsonify({"id": "1092", "message": "Data Query Error", "description": "", "data": "", "success": False})

        obj = mod_concent_management_functions.ClsConcentManagementFunc(db_client_waba_settings)
        response = obj.func_get_recipient_details(data)
        return response
    

    @app_concent_api.route('/update_recepient_details', methods=['POST'])
    @token_required
    def update_recepient_details():
        data = request.json
        claims = get_jwt()
        bot_id = claims["bot_id"]
        data = request.json
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)
        except Exception as e:
            lg.critical("bot_id =" + str(bot_id) + " | " + "DB error - client_waba_settings : " + str(e))    
            return jsonify({"id": "1092", "message": "Data Query Error", "description": "", "data": "", "success": False})

        obj = mod_concent_management_functions.ClsConcentManagementFunc(db_client_waba_settings)
        response = obj.func_update_recipient_details(data)
        return response
    

    @app_concent_api.route('/status_change', methods=['GET'])
    @token_required
    def status_change():
        claims = get_jwt
        bot_id = claims["bot_id"]
        data = request.json
        try:
            obj_common = ClsCommon()
            db_client_waba_settings = obj_common.get_waba_settings_by_bot_id(bot_id)        
        except Exception as e:    
            lg.critical("bot_id =" + str(bot_id) + " | " + "DB error - client_waba_settings : " + str(e))    
            return jsonify({"id": "1092", "message": "Data Query Error", "description": "", "data": "", "success": False})

        from_date = request.args.get('from_date')
        to_date = request.args.get('to_date')
        obj = mod_concent_management_functions.ClsConcentManagementFunc(db_client_waba_settings)
        response = obj.func_recipient_status_change(from_date, to_date)

        return response

        

