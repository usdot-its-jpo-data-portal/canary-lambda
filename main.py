import boto3
import datetime
import json
import queue
from decimal import Decimal
from odevalidator import TestCase

### Data source config
CONFIG_FILE = "./bsmLogDuringEvent.ini"
S3_BUCKET = "usdot-its-cvpilot-public-data"
DATA_PROVIDER = "wydot"
MESSAGE_TYPE = "BSM"
#MESSAGE_TYPE = "TIM"
SAMPLE_SIZE = 10

### Outbound email alert settings
SEND_EMAIL_ALERTS = False
SOURCE_EMAIL = "fake@email.com"

def lambda_handler(event, context):
    s3_client = boto3.client('s3')

    ddate = datetime.datetime.now()
    prefix_string = "%s/%s/%s/%s/%s" % (DATA_PROVIDER, MESSAGE_TYPE, ddate.year, str(ddate.month).zfill(2), str(ddate.day).zfill(2))

    s3_file_list = list_s3_files_matching_prefix(s3_client, prefix_string)

    msg_queue = queue.Queue()
    for filename in s3_file_list:
        record_list = extract_records_from_file(s3_client, filename)
        [msg_queue.put(record) for record in record_list]


    # print(list(msg_queue.queue))

    test_case = TestCase(CONFIG_FILE)
    validation_results = test_case.validate_queue(msg_queue)

    if list_contains_no_errors(validation_results['Results']):
        print("No validation errors detected.")
    else:
        print("Validation failed, errors detected.")
        if SEND_EMAIL_ALERTS:
            print("Sending email alert.")
            send_report("Validation failed, detected %d errors out of %d messages analyzed. ")
        else:
            print("Sending email alert.")
    return

###
def send_report(message):
    ses_client = boto3.client('ses')
    response = ses_client.send_email(
        Source=SOURCE_EMAIL,
        Destination={
            'ToAddresses': [
                DESTINATION_EMAIL,
            ],
            'CcAddresses': [
            ],
            'BccAddresses': [
            ],
        },
        Message={
            'Subject': {
                'Charset': 'UTF-8',
                'Data': '[DATAHUB AUTOMATED ALERT] DataHub Canary Validation Lambda Results'
            },
            'Body': {
                'Text': {
                    'Charset': 'UTF-8',
                    'Data': message
                }
            }
        },
        ReplyToAddresses=[
        ],
        ReturnPath='',
        SourceArn='',
        ReturnPathArn='',
    )

###
def list_contains_no_errors(result_list):
    for result in result_list:
        print(json.dumps(result))
        for validation in result['Validations']:
            if validation['Valid'] == False:
                return False
    return True

### Returns a list of records from a given file
def extract_records_from_file(s3_client, filename):
    s3_file = s3_client.get_object(
        Bucket=S3_BUCKET,
        Key=filename,
    )
    return s3_file['Body'].read().splitlines()

### Returns filenames from an S3 list files (list_objects) query
def list_s3_files_matching_prefix(s3_client, prefix_string):
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET,
        MaxKeys=SAMPLE_SIZE,
        Prefix=prefix_string,
    )
    filenames = []
    [filenames.append(item['Key']) for item in response.get('Contents')]
    return filenames
