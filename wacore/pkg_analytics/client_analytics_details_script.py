from pymongo import MongoClient
print("ok")
# url = "mongodb://Himanshu:Himanshu123@4.188.251.193:27017/admin" 

url = "mongodb://prashant:BandBand@20.204.129.114:3004/"


client = MongoClient(url)
import pandas as pd

# client_db = client["ew54_223422024192382"]
client_db = client["ew5_107489541996167"]


import datetime

current_timestamp = int(datetime.datetime.now().timestamp())
dt_object_cur = datetime.datetime.utcfromtimestamp(current_timestamp)
date_cur = dt_object_cur.strftime("%d")
month_cur = dt_object_cur.strftime("%m")
year_cur = dt_object_cur.strftime("%Y")
datemonth_cur = str(date_cur+month_cur+year_cur)

print(datemonth_cur)

query = {
        "date_month":datemonth_cur
    }

projection = {'template_name': 1, '_id': 0}

template_list = list(client_db.client_sent_to_wa_analytics.find(query,projection))

print(len(template_list))

template_names = [doc['template_name'] for doc in template_list]
print(f"list of templates are {template_names} and length is {len(template_names)}")



# broad_list = [1710934602]

for temp_name in template_names:

    print(temp_name)

   

    delivered_count = client_db.client_sent_to_wa_analytics_info.find({"template_name": temp_name,"date_month":datemonth_cur, "delivered": {'$in': [True]}})
    delivered_count = len(list(delivered_count))
    print(f"this is delivered_count {delivered_count}")

    read_count = client_db.client_sent_to_wa_analytics_info.find({"template_name": temp_name,"date_month":datemonth_cur, "read": {'$in': [True]}})
    read_count = len(list(read_count))
    print(f"this is read_count  {read_count}")





    query_condition = {"template_name": temp_name,"date_month":datemonth_cur}

    update_query = {'$set': {
          'recipient_delivered_count': delivered_count,
          'recipient_read_count': read_count
      }}

    updated_document = client_db.client_sent_to_wa_analytics.find_one_and_update(
          query_condition, update_query,
          return_document=True
      )