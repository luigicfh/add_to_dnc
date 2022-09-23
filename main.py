import functions_framework
from google.cloud import firestore
from handler_cf_v1 import services, utils
import os
import traceback

ENV_VAR_MSG = "Specified environment variable is not set."


def notify_success(markup):
    sender = os.environ.get('SENDER', ENV_VAR_MSG)
    password = os.environ.get('PASSWORD', ENV_VAR_MSG)
    recipients = os.environ.get('RECIPIENTS', ENV_VAR_MSG).split(",")
    subject = f"AT Central Notifications | Numbers Added to DNC"

    body = f"""
    DNC Numbers Report: <br>
    {markup}
    """
    return utils.send_email(sender, password, recipients, subject, body)


def notify_error(error):
    sender = os.environ.get('SENDER', ENV_VAR_MSG)
    password = os.environ.get('PASSWORD', ENV_VAR_MSG)
    recipients = os.environ.get('RECIPIENTS', ENV_VAR_MSG).split(",")
    subject = f"AT Central Notifications | DNC Job Failure"

    body = f"""
    This message is to inform you that the scheduled task for DNC numbers has failed with the following error: <br>
    {error}
    """
    return utils.send_email(sender, password, recipients, subject, body)


@functions_framework.http
def add_to_dnc(request):

    project = os.environ.get("PROJECT", ENV_VAR_MSG)
    services_collection = os.environ.get("COLLECTION", ENV_VAR_MSG)
    service_instance_id = os.environ.get("SERVICE_INSTANCE", ENV_VAR_MSG)

    db = firestore.Client(project)

    service_instance_doc = utils.get_doc(
        db, services_collection, service_instance_id)

    app = getattr(services, service_instance_doc['appClassName'])

    service = getattr(services, service_instance_doc['className'])

    dnc_jobs = db.collection(service_instance_doc['dncCollection']).where(
        'state', '==', 'queued').get()

    if len(dnc_jobs) == 0:
        return "OK"

    dnc_jobs_to_dict = [dnc_job.to_dict() for dnc_job in dnc_jobs]

    numbers = []

    for number in dnc_jobs_to_dict:

        numbers.extend(number['numbers'])

    service_instance = service(service_instance_doc, {}, app)

    try:
        service_instance.add_to_dnc(numbers)
    except Exception as e:
        service_instance.handle_error(traceback.format_exc(), notify_error)

    markup = service_instance.handle_success(
        dnc_jobs_to_dict, dnc_jobs, db, service_instance_doc['dncCollection'])

    notify_success(markup)

    return "OK"
