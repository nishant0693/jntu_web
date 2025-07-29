import os.path
import datetime
import requests
import json
from flask import jsonify
from urllib.parse import urlparse
from datetime import datetime, timedelta
from pymongo.errors import BulkWriteError

# Import custom packages
from ..pkg_db_connect.mod_db_connection import ClsMongoDBInit
from ..pkg_analytics.mod_analytics_function import ClsDmpServiceAnalytics
from wacore.pkg_extras.mod_common import ClsCommon


class ClsConcentManagementFunc:


    def __init__(self,db_client_waba_settings):
        """ Create or initialize object and variables """
        self.db_client_waba_settings = db_client_waba_settings
        try:
            self.client_db_name = db_client_waba_settings["response"]["ew_id"].lower() + "_" + db_client_waba_settings["response"]["waba_id"]
        except:
            self.client_db_name = ""
        db_client_waba_settings["response"]["ew_id"].lower() + "_" + db_client_waba_settings["response"]["waba_id"]
        
        self.cl_db = ClsMongoDBInit.get_cl_db_client(self.client_db_name)
        # self.cl_db = cl_db
        self.cl_collectionn = self.cl_db["concent_details"]
            
        


    def func_get_recipient_details(self, data):

        if data and data.get("recipient_number") != None:

            # creating mongo insert data
            recipient_number = data.get("recipient_number")
        
        else:
            return jsonify({ "data": "" , 
                            "message": "payload in not correct", 				
                            "description": "", 
                            "id": "4001", 
                            "success": False 
                            }), 400

        try:
            db_obj = self.cl_collectionn.find({"_id": { "$in": recipient_number}}, {"_id":0})
            final_data = []

            for result in db_obj:
                final_data.append(result)

            return jsonify({"data": final_data, "status": True,
                            "message": "recipient details fetched successfully",
                            "description": "",
                            "id": "3457",
                            "success": True}), 200

        except  Exception as e:
            return jsonify({ "data": "" , 
                            "message": str(e), 				
                            "description": "server side issue", 
                            "id": "3457", 
                            "success": False 
                            }), 500


    def func_update_recipient_details(self, data):
        update_data = []

        if data and data.get("recipient_list") != None:
            recipient_list = data.get("recipient_list")

            for recipient in recipient_list:
                if recipient.get("recipient_number") != None and recipient.get("concent_flag") != None:
                    update_data.append({"recipient_number": recipient.get("recipient_number"), "concent_flag": recipient.get("concent_flag"), "updated_at": int(datetime.now().timestamp())})

        else:
            return jsonify({ "data": "" , 
                            "message": "payload in not correct", 				
                            "description": "", 
                            "id": "3457", 
                            "success": False 
                            }), 400
        

        try:
            for update in update_data:
                query = {"_id": update.get("recipient_number")}
                new_val = {"$set": update}
                print(query, new_val)
                self.cl_collectionn.update_one(query, new_val)


            return jsonify({"data": "", "status": True,
                            "message": "recipient details updated successfully",
                            "description": "",
                            "id": "3457",
                            "success": True}), 200
            

        except  Exception as e:
            return jsonify({ "data": "" , 
                            "message": str(e), 				
                            "description": "server side issue", 
                            "id": "3457", 
                            "success": False 
                            }), 500     
        


    def func_recipient_status_change(self, from_date, to_date):
        if from_date and to_date:
            query = {
                'updated_at': {
                    '$gte': int(datetime.strptime(from_date, "%Y-%m-%d").timestamp()),
                    '$lte': int(datetime.strptime(to_date, "%Y-%m-%d").timestamp())
                }
            }

            try:
                cursor = self.cl_collectionn.find(query, {'_id': 0})
                final_data = []
                for cur in cursor:
                    final_data.append(cur)

                
                return jsonify({"data": final_data, "status": True,
                                "from_date": str(from_date), "to_date" : str(to_date),
                            "message": "recipient details fetched successfully",
                            "description": "",
                            "id": "3457",
                            "success": True}), 200
            
            except  Exception as e:
                return jsonify({ "data": "" , 
                                "message": str(e), 				
                                "description": "server side issue", 
                                "id": "3457", 
                                "success": False 
                                }), 500


        else:
            return jsonify({ "data": "" , 
                            "message": "query params not present, please send 'from' and 'to' in query params",
                            "id": "3457", 
                            "success": False 
                            }), 400


    
    def func_add_recipients(self, data):

        insert_data =[]
        # self.client_db.vishnu_check.insert_one({"dev":data})

        # if data and data.get("recipient_number") != None:
        if data and data['recipient_number'] != None:       
            # creating mongo insert data
            for number in data.get("recipient_number"):
                insert_data.append({"_id": number,"recipient_number": number, "concent_flag": True, "updated_at": int(datetime.now().timestamp())})
        
        else:
            return jsonify({ "data": "" , 
                            "message": "payload in not correct", 				
                            "description": "", 
                            "id": "3457", 
                            "success": False 
                            }), 400


        try:
            # collection.insert_many([{"recipient_number": 8889993332, "concent_flag": True, "updated_at": datetime.now()},
            #                         {"recipient_number": 7779993332, "concent_flag": True, "updated_at": datetime.now()}])

            # self.cl_collectionn.insert_many(insert_data, ordered=False)

            # for d in insert_data:
            #     query = {"_id": d.get("recipient_number")}
            #     new_val = {"$set": d}
            #     print(query, new_val)
            #     self.cl_collectionn.update_one(query, new_val, upsert=True)

            # Insert the documents
            result = self.cl_collectionn.insert_many(insert_data, ordered=False)
            print('Inserted document IDs:', result.inserted_ids)

            

            return jsonify( { "data": "" , 
                            "message": "recipients added successfully", 				
                            "description": "", 
                            "id": "3457", 
                            "success": True 
                            } )

        except BulkWriteError as e:
            duplicate_number = []
            for error in e.details['writeErrors']:
                if error['code'] == 11000:
                    print(f"Skipped duplicate entry with _id: {error['op']['_id']} and number: {error['op']['recipient_number']}")
                    duplicate_number.append(error['op']['recipient_number'])  
                else:
                    print(f"ERROR: {e}")

            return jsonify({ "data": "" , 
                        "message": "recipients added successfully", 				
                        "description": f"Skipped duplicate entry: {duplicate_number}", 
                        "id": "3457", 
                        "success": False 
                        } )

        except Exception as e:
            print(f"ERROR: {e}")
            return jsonify({ "data": "" , 
                            "message": "contact insertion failed..", 				
                            "description": str(e), 
                            "id": "3457", 
                            "success": False 
                            } )
        

    def number_with_concent_false(self):
        try:
            query = {'concent_flag': False}
                

            cursor = self.cl_collectionn.find(query, {'_id': 0})
            final_data = []
            for cur in cursor:
                final_data.append(cur.get('recipient_number'))


            return final_data
        except Exception as e:
            print("error in fetching false concent data")
            return []

    
    