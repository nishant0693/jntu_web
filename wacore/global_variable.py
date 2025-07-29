"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: All Global Variables for wacore Package
    * Description: 
        # Only define Global dotenv variables in this file  
        # MongoDB Database variables 
"""

import os
from dotenv import load_dotenv

"""
========================================================
============== Load Global Variable .env Loading
========================================================
"""
env_path = os.path.abspath(os.path.dirname(__file__))+'/../.env'
load_dotenv(dotenv_path=env_path)


"""
========================================================
============== MongoDB Global Variable 
========================================================
"""
connection_string = os.getenv("MONGO_CONNECTION_STRING")
database_name = os.getenv("EW_DB")
# print("\n\n\n\n Global  DB Vars Called \n\n\n\n")

"""
========================================================
============== Local Global Variable 
========================================================
"""
graph_url = "https://graph.facebook.com/v16.0/"
channel_dtls_db = "channel_details"
url_for_dmp = os.getenv("DMP_URL")