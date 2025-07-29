"""
 * 
 * Main Entry File
 * Description: 
 *
"""
from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
from dotenv import load_dotenv
import logging
import logging.config
import os
import socket
from datetime import datetime, timedelta
from flask_jwt_extended import (
    JWTManager,
)
# import yaml


"""
============================================
============== Import All Blueprint App Here
============================================ 
"""
from .pkg_template import mod_template_routes
from .pkg_profile import mod_profile_routes
from .pkg_waba_management import mod_waba_managemnt_routes
from .pkg_webhook import mod_webhook_routes
from .pkg_analytics import mod_analytics_routes
from .pkg_login import mod_whatsapp_login_routes
from .pkg_appwebhook import mod_app_webhook_routes
from .pkg_concent_management import mod_concent_management


"""
===============================
============== Global Variables
=============================== 
"""
c=0
hostname = socket.gethostname()
ip_address = socket.gethostbyname(hostname)


"""
=====================================
============== Global App and Configs
===================================== 
"""

app = Flask(__name__,static_url_path="/static")
jwt = JWTManager(app)
app.config["JWT_SECRET_KEY"] = "secret_key"
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(minutes=480)
app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(minutes=480)
app.config['CORS_HEADERS'] = 'Content-Type'
#cors = CORS(app, resources={r"/": {"origins": ""}})
CORS(app)


"""
==============================================
============== Register All Blueprint App Here
============================================== 
"""
app.register_blueprint(mod_template_routes.app_templates)
app.register_blueprint(mod_profile_routes.app_profile)
app.register_blueprint(mod_waba_managemnt_routes.app_waba_management)
app.register_blueprint(mod_webhook_routes.app_webhook)
app.register_blueprint(mod_analytics_routes.app_analytics)
app.register_blueprint(mod_whatsapp_login_routes.app_login)
app.register_blueprint(mod_app_webhook_routes.app_webhook)
app.register_blueprint(mod_concent_management.app_concent_api)
"""
====================================
============== Load Logger Settings 
====================================
"""
# with open('conf/logging.yaml', 'r') as f:
#     config = yaml.safe_load(f.read())
#     logging.config.dictConfig(config)
# LOG = logging.getLogger('appLog')



"""
====================
============== API's
====================
"""

@app.get('/waapi')
def index():
    global c
    """Index Route"""
    c=c+1
    r = "Count="+str(c)+" :: PID="+str(pid) +":: IP="+str(ip_address)+ ":: HOSTNAME="+str(hostname)
    return str((r))

"""
========================================================
============== Load as Gunicorn server or python app 
========================================================
"""

if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    print("Started as GUNICORN")
    pid = os.getpid()
else:
    print("------> Started as APP")
    if __name__ == "__main__":
        # app.run(debug=True)
        app.run(host='0.0.0.0', port=5001, debug=True)
