"""
    * Copyright (C) engagely.ai - All Rights Reserved 
    * About File: Contains schemas of DMP embedded signup package only
    * Description: All the Embedded signup schemas present here
"""

from typing import Optional, List
from pydantic import BaseModel

class SchBmid(BaseModel):
    """ Class called to declare schema for BMID """
    bmid: str

class SchGetBmid(BaseModel):
    """ Class called to declare schema for get BMID """
    bot_id: str

class SchWabaId(BaseModel):
    """ Class called to declare schema for waba id """
    waba_id: str

class SchAllIds(BaseModel):
    """ Class called to declare schemas for all ids """
    bot_id: str
    waba_id: str = None
    ew_id: str = None
    bot_id: str = None

class SchWAPhoneNumbers(BaseModel):
    """ Class called to declare schemas for whatsapp phone numbers """
    country_code: str
    wa_number: int
    wa_display_name: str
    
class SchClientBusinessInfo(BaseModel):
    """ Class called to declare schemas for client business info """
    bmid: str
    waba_id: str = None
    ew_id: str = None
    bot_id: str = None
    business_name : str
    business_hq : str
    business_address : str
    business_websites : str
    business_description : str
    business_email : str
    business_phone_number : str
    business_vertical : str
    wa_phone_numbers: List[SchWAPhoneNumbers] = None

class SchGetDebugToken(BaseModel):
    """ Class called to declare schemas for get debug token """
    oauth_user_token : str
    bmid : str

class SchValidateNumber(BaseModel):
    """ Class called to declare schemas for validate phone numbers """
    phone_number : str