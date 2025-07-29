from pymongo import MongoClient
print("ok")
# url = "mongodb://Himanshu:Himanshu123@4.188.251.193:27017/admin" 

url = "mongodb://prashant:BandBand@20.204.129.114:3004/"

client = MongoClient(url)

import pandas as pd

# client_db = client["ew51_137727896080628"]

ew_id = "ew5_107489541996167"

client_db = client[ew_id]

from datetime import datetime, timezone , timedelta

current_date_utc = datetime.now(timezone.utc)


#starttime in utc
start_of_day_utc = datetime(current_date_utc.year, current_date_utc.month, current_date_utc.day, 0, 0, 0, tzinfo=timezone.utc)

#yesterday_date_utc = current_date_utc - timedelta(days=1)

#yesterday time  in UTC
#start_of_yesterday_utc = datetime(yesterday_date_utc.year, yesterday_date_utc.month, yesterday_date_utc.day, 0, 0, 0, tzinfo=timezone.utc)


#endtime in utc
end_of_day_utc = datetime(current_date_utc.year, current_date_utc.month, current_date_utc.day, 23, 59, 59, 999999, tzinfo=timezone.utc)

# start_timestamp_utc = int(start_of_day_utc.timestamp())


start_timestamp_utc = int(start_of_day_utc.timestamp())
end_timestamp_utc = int(end_of_day_utc.timestamp())

print(type(start_timestamp_utc))


print("Start timestamp (UTC):", start_timestamp_utc)
print("End timestamp (UTC):", end_timestamp_utc)

query = {
    'timestamp': {
        '$gte': start_timestamp_utc,
        '$lte': end_timestamp_utc
    }
}

projection = {'broadcast_id': 1, '_id': 0}

broadcast_id_List = list(client_db.broadcast_details.find(query,projection))

broadcast_ids = [doc['broadcast_id'] for doc in broadcast_id_List]
print(f"list of broadcast_ids are {broadcast_ids} and length is {len(broadcast_ids)}")



# broad_list = [1710934602]

for broadcast_id in broadcast_ids:

    print(broadcast_id)

   

    delivered_count = client_db.broadcast_details_info.find({"broadcast_id": broadcast_id, "delivered": {'$in': [True]}})
    delivered_count = len(list(delivered_count))
    print(f"this is delivered_count {delivered_count}")

    read_count = client_db.broadcast_details_info.find({"broadcast_id": broadcast_id, "read": {'$in': [True]}})
    read_count = len(list(read_count))
    print(f"this is read_count  {read_count}")





    query_condition = {"broadcast_id": broadcast_id}

    update_query = {'$set': {
          'recipient_delivered_count': delivered_count,
          'recipient_read_count': read_count
      }}

    updated_document = client_db.broadcast_details.find_one_and_update(
          query_condition, update_query,
          return_document=True
      )