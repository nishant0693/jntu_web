from pymongo import MongoClient
import datetime

# url = "mongodb://Himanshu:Himanshu123@4.188.251.193:27017/admin" 
url = "mongodb://prashant:BandBand@20.204.129.114:3004/"

current_utc_date = datetime.datetime.utcnow().date()

start_of_day_utc_timestamp = int(datetime.datetime(current_utc_date.year, current_utc_date.month, current_utc_date.day, 0, 0, 0).timestamp())

# End of day UTC timestamp
end_of_day_utc_timestamp = int(datetime.datetime(current_utc_date.year, current_utc_date.month, current_utc_date.day, 23, 59, 59).timestamp())

start_of_day_utc_timestamp = str(start_of_day_utc_timestamp)
end_of_day_utc_timestamp = str(end_of_day_utc_timestamp)

print(f"Start of day UTC Unix timestamp: {start_of_day_utc_timestamp}")
print(f"End of day UTC Unix timestamp: {end_of_day_utc_timestamp}")

client = MongoClient(url)
# ew_id = "ew54_223422024192382"

ew_id = "ew5_107489541996167"


client_db = client[ew_id]

list_of_collections = client_db.list_collection_names()
check_db_list = ['client_sent_to_wa_analytics_info', 'client_sent_to_wa_analytics', 'unknown_status']

if all(collection in list_of_collections for collection in check_db_list):
    unknown_list = list(client_db.unknown_status.find({"data.statuses.timestamp": {"$gte": start_of_day_utc_timestamp}}))
    print(f"Number of documents missed: {len(unknown_list)}")

    for unknown_document in unknown_list:
        unknown_doc = unknown_document.get('data',{})
        print(f"unknown_document found is {unknown_doc}")
        # statuses = unknown_document.get("data", {}).get("statuses", [])
        # statuses = statuses[0]
        # msg_id = statuses.get('id'," ")
        # unknown_status = statuses.get("status"," ")
        for entry in unknown_doc.get('entry', []): 
            for change in entry.get('changes', []):  
                if 'value' in change and isinstance(change['value'], dict):
                    if 'statuses' in change['value']:
                        unknown_status = change['value']['statuses'][0]['status']
                        print(unknown_status)
                        msg_id = change['value']['statuses'][0]['id']
                        lst_statuses = change['value']['statuses'][0]
                        break
        print(f"The status which we missed is {unknown_status}")
        if msg_id ==" " or unknown_status == " ":
            pass
        if msg_id and unknown_status:
            matched_id_bdi = client_db.client_sent_to_wa_analytics_info.find({"message_response_id": msg_id})
            matched_id_bdi_list = list(matched_id_bdi)

            for doc in matched_id_bdi_list:
                template_name = doc.get('template_name', "")

                if unknown_status not in doc:
                    if unknown_status == "delivered":
                        if 'delivered' not in doc:
                            client_db.client_sent_to_wa_analytics_info.update_one(
                                {"message_response_id": msg_id, "template_name": template_name},
                                {"$set": {"delivered": True}})
                    elif unknown_status == "read":
                        if 'delivered' not in doc:
                            if 'read' not in doc:
                                client_db.client_sent_to_wa_analytics_info.update_one(
                                    {"message_response_id": msg_id, "template_name": template_name},
                                    {"$set": {"delivered": True, "read": True}})
                        else:
                            if 'read' not in doc:
                                client_db.client_sent_to_wa_analytics_info.update_one(
                                    {"message_response_id": msg_id, "template_name": template_name},
                                    {"$set": {"read": True}})
                    # elif unknown_status == "failed":
                    #     if 'reason' not in doc:
                    #         client_db.client_sent_to_wa_analytics_info.update_one(
                    #             {"message_response_id": msg_id, "template_name": template_name},
                    #             {"$set": {"reason": unknown_status}})
                    else:
                        pass

            client_db.unknown_status.delete_one({"data.statuses.id": msg_id})

print("All documents are updated")
