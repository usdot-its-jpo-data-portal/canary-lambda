import boto3
import datetime
import json
import queue
from decimal import Decimal
from odevalidator import TestCase

### Data source configuration settings
USE_STATIC_PREFIX = True
STATIC_PREFIX = "wydot/BSM/2019/04"

### Data source config
CONFIG_FILE = "./bsmLogDuringEvent.ini"
CHECKABLE_FILE_PREFIX = "bsmLogDuringEvent"
S3_BUCKET = "usdot-its-cvpilot-public-data"
DATA_PROVIDER = "wydot"
MESSAGE_TYPE = "BSM"
SAMPLE_SIZE = 10

### Outbound email alert settings
SEND_EMAIL_ALERTS = False
SOURCE_EMAIL = "sender@email.com"
DESTINATION_EMAIL = "receiver@email.com"

def lambda_handler(event, context):
    test_case = TestCase(CONFIG_FILE)
    s3_client = boto3.client('s3')

    prefix_string = ""
    if USE_STATIC_PREFIX:
        prefix_string = STATIC_PREFIX
    else:
        ddate = datetime.datetime.now()
        prefix_string = "%s/%s/%s/%s/%s" % (DATA_PROVIDER, MESSAGE_TYPE, ddate.year, str(ddate.month).zfill(2), str(ddate.day).zfill(2))

    s3_file_list = list_s3_files_matching_prefix(s3_client, prefix_string)
    print("[INFO] Queried for %d S3 files matching prefix string. Found %d matching files." % (len(s3_file_list), SAMPLE_SIZE))
    print("[INFO] Matching files: [%s]" % ", ".join(s3_file_list))

    log_file_list = []
    msg_queue = queue.Queue()
    for filename in s3_file_list:
        print("[INFO] Analyzing file '%s'" % filename)
        record_list = extract_records_from_file(s3_client, filename)
        for record in record_list:
            log_file_name = json.loads(record)['metadata']['logFileName']
            if log_file_name not in log_file_list:
                log_file_list.append(log_file_name)
            if CHECKABLE_FILE_PREFIX in log_file_name:
                msg_queue.put(record)

        print("[INFO] Log files found in current S3 file: [%s]" % ", ".join(log_file_list))
        print("[INFO] Analyzable records in current S3 file: %d out of %d" % (msg_queue.qsize(), len(record_list)))

        if msg_queue.qsize() == 0:
            print("[WARNING] ============================================================================")
            print("[WARNING] Could not find any records to be validated in S3 file '%s'." % filename)
            print("[WARNING] ============================================================================")
            continue

        validation_results = test_case.validate_queue(msg_queue)

        num_errors = 0
        num_validations = 0
        error_dict = {}
        for result in validation_results['Results']:
            num_validations += len(result['Validations'])
            for validation in result['Validations']:
                if validation['Valid'] == False:
                    num_errors += 1
                    invalid_field = validation['Field']
                    if invalid_field in error_dict:
                        error_dict[invalid_field] += 1
                    else:
                        error_dict[invalid_field] = 1

        if num_errors > 0:
            print("[FAILED] ============================================================================")
            print("[FAILED] S3 Filename: %s" % filename)
            print("[FAILED] Validation has failed! Detected %d errors out of %d total validation checks." % (num_errors, num_validations))
            for error in error_dict:
                print("[FAILED] Field: '%s', Errors: '%d'" % (error, error_dict[error]))
            print("[FAILED] ============================================================================")
        else:
            print("[SUCCESS] ===========================================================================")
            print("[SUCCESS] S3 Filename: %s" % filename)
            print("[SUCCESS] Validation has passed. Detected no errors out of %d total validation checks." % (num_validations))
            print("[SUCCESS] ===========================================================================")

    return

    # if list_contains_no_errors(validation_results['Results']):
    #     print("No validation errors detected.")
    # else:
    #     print("Validation failed, errors detected.")
    #     if SEND_EMAIL_ALERTS:
    #         print("Sending email alert.")
    #         send_report("Validation failed, detected %d errors out of %d messages analyzed. ")
    #     else:
    #         print("Sending email alert.")
    # return

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
    if response.get('Contents'):
        [filenames.append(item['Key']) for item in response.get('Contents')]
    return filenames

if __name__ == '__main__':
    lambda_handler(None, None)
