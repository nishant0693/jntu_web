import os
from celery import Celery
from dotenv import load_dotenv 

# Import custom packages
from wacore.pkg_template_broadcast.mod_celery_broadcast2 import func_send_message_request


load_dotenv(".env")

celery = Celery(__name__)

celery.conf.update(
    result_expires=15,
    task_acks_late=True,
    broker_url = os.environ.get("CELERY_BROKER_URL"),
    result_backend = os.environ.get("CELERY_RESULT_BACKEND"),
    task_serializer='json'
)


#-------------------------------------------------------new single msg send task
@celery.task(name="sending_tasks")
def async_broadcast_single_template(var_message_body,str_recipient_number,str_url_templates,wa_token,client_number,broadcast_id,broadcast_name):
    var_contact_status = func_send_message_request(var_message_body,str_recipient_number,str_url_templates,wa_token,client_number,broadcast_id,broadcast_name)
    return "Single MSG Finished"
#-------------------------------------------------------
