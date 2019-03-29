import boto3
import datetime
import dateutil.parser
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

    record_list = []
    for filename in s3_file_list:
        record_list.extend(extract_records_from_file(s3_client, filename))

    external_validation_results = perform_external_validations(record_list)
    internal_validation_results = perform_internal_validations(record_list)

    # if list_contains_no_errors(external_validation_results['Results']):
    #     print("No validation errors detected.")
    # else:
    #     print("Validation failed, errors detected.")
    #     if SEND_EMAIL_ALERTS:
    #         print("Sending email alert.")
    #         send_report("Validation failed, detected %d errors out of %d messages analyzed. ")
    #     else:
    #         print("Sending email alert.")
    return

### Validate message list using the validation library
def perform_external_validations(msg_list):
    msg_queue = queue.Queue()
    [msg_queue.put(msg) for msg in msg_list]
    test_case = TestCase(CONFIG_FILE)
    return test_case.validate_queue(msg_queue)

### Validate things not covered by the validation library
def perform_internal_validations(msg_list):
    sorted_list = sorted(msg_list, key=lambda msg: (json.loads(msg)['metadata']['logFileName'], json.loads(msg)['metadata']['serialId']['recordId']))
    sorted_list = sorted(msg_list, key=lambda msg: (json.loads(msg)['metadata']['recordId'], json.loads(msg)['metadata']['serialId']['logFileName']))
    perform_sequential_validations(sorted_list)

### Iterate messages and check that sequential items are sequential
def perform_sequential_validations(record_list):
    old_log_file_name = json.loads(record_list[0])['metadata']['logFileName']
    old_record_id = int(json.loads(record_list[0])['metadata']['serialId']['recordId'])
    old_serial_number = int(json.loads(record_list[0])['metadata']['serialId']['serialNumber'])
    old_record_generated_at = dateutil.parser.parse(json.loads(record_list[0])['metadata']['recordGeneratedAt'])
    old_ode_received_at = dateutil.parser.parse(json.loads(record_list[0])['metadata']['odeReceivedAt'])
    for record in record_list[1:]:
        new_log_file_name = json.loads(record)['metadata']['logFileName']
        new_record_id = int(json.loads(record)['metadata']['serialId']['recordId'])
        new_serial_number = int(json.loads(record)['metadata']['serialId']['serialNumber'])
        new_record_generated_at = dateutil.parser.parse(json.loads(record)['metadata']['recordGeneratedAt'])
        new_ode_received_at = dateutil.parser.parse(json.loads(record)['metadata']['odeReceivedAt'])
        if old_log_file_name == new_log_file_name:
            if new_record_id != old_record_id+1:
                print("WARNING! Detected incorrectly incremented recordId. Expected '%d' but got '%d'" % (old_record_id+1, new_record_id))
            if new_serial_number != old_serial_number+1:
                print("WARNING! Detected incorrectly incremented serialNumber. Expected '%d' but got '%d'" % (old_serial_number+1, new_serial_number))
            if new_record_generated_at < old_record_generated_at:
                print("WARNING! Detected non-chronological recordGeneratedAt. Previous timestamp was '%s' but current timestamp is '%s'" % (old_record_generated_at, new_record_generated_at))
            if new_ode_received_at < old_ode_received_at:
                print("WARNING! Detected non-chronological odeReceivedAt. Previous timestamp was '%s' but current timestamp is '%s'" % (old_ode_received_at, new_ode_received_at))
        else:
            print("New log file detected. Resetting old item values. Old filename: '%s', new filename: '%s'" % (old_log_file_name, new_log_file_name))
        old_log_file_name = new_log_file_name
        old_record_id = new_record_id
        old_serial_number = new_serial_number
        old_record_generated_at = new_record_generated_at
        old_ode_received_at = new_ode_received_at

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
