import boto3
import datetime
import json
import queue
import logging
from decimal import Decimal
from odevalidator import TestCase

### Data source configuration settings
USE_STATIC_PREFIX = False
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
INCLUDE_VERBOSE_REPORT_AS_ATTACHMENT = True
SOURCE_EMAIL = "sender@email.com"
DESTINATION_EMAIL = "receiver@email.com"

def lambda_handler(event, context):

    # Setup logger and if specified, write logs to output file
    root = logging.getLogger()
    if root.handlers: # Remove default AWS Lambda logging configuration
        for handler in root.handlers:
            root.removeHandler(handler)
    logger = logging.getLogger('canary')
    logging.basicConfig(format='%(levelname)s %(message)s')
    if INCLUDE_VERBOSE_REPORT_AS_ATTACHMENT:
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.FileHandler('/tmp/validation_results.log', 'w'))
    else:
        logger.setLevel(logging.INFO)

    # Begin validation routine
    test_case = TestCase(CONFIG_FILE)
    s3_client = boto3.client('s3')

    prefix_string = ""
    if USE_STATIC_PREFIX:
        prefix_string = STATIC_PREFIX
    else:
        ddate = datetime.datetime.now()
        prefix_string = "%s/%s/%s/%s/%s" % (DATA_PROVIDER, MESSAGE_TYPE, ddate.year, str(ddate.month).zfill(2), str(ddate.day).zfill(2))

    s3_file_list = list_s3_files_matching_prefix(s3_client, prefix_string)
    logger.info("Queried for %d S3 files matching prefix string '%s'. Found %d matching files." % (SAMPLE_SIZE, prefix_string, len(s3_file_list)))
    logger.info("Matching files: [%s]" % ", ".join(s3_file_list))

    log_file_list = []
    total_validation_count = 0
    total_validations_failed = 0
    msg_queue = queue.Queue()
    for filename in s3_file_list:
        logger.info("Analyzing file '%s'" % filename)
        record_list = extract_records_from_file(s3_client, filename)
        for record in record_list:
            log_file_name = json.loads(record)['metadata']['logFileName']
            if log_file_name not in log_file_list:
                log_file_list.append(log_file_name)
            if CHECKABLE_FILE_PREFIX in log_file_name:
                msg_queue.put(record)

        logger.info("Log files found in current S3 file: [%s]" % ", ".join(log_file_list))
        logger.info("Analyzable records in current S3 file: %d out of %d" % (msg_queue.qsize(), len(record_list)))

        if msg_queue.qsize() == 0:
            logger.warning("============================================================================")
            logger.warning("Could not find any records to be validated in S3 file '%s'." % filename)
            logger.warning("============================================================================")
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
                    validation_message = validation['Details']
                    if validation_message in error_dict:
                        error_dict[validation_message] += 1
                    else:
                        error_dict[validation_message] = 1

        total_validation_count += num_validations
        total_validations_failed += num_errors
        if num_errors > 0:
            logger.error("[FAILED] ============================================================================")
            logger.error("[FAILED] S3 Filename: %s" % filename)
            logger.error("[FAILED] Validation has failed! Detected %d errors out of %d total validation checks." % (num_errors, num_validations))
            for error in error_dict:
                logger.debug("[FAILED] Error: '%s', Occurrences: '%d'" % (error, error_dict[error]))
            logger.error("[FAILED] ============================================================================")
        else:
            logger.info("[SUCCESS] ===========================================================================")
            logger.info("[SUCCESS] S3 Filename: %s" % filename)
            logger.info("[SUCCESS] Validation has passed. Detected no errors out of %d total validation checks." % (num_validations))
            logger.info("[SUCCESS] ===========================================================================")

    logger.info("[CANARY FINISHED] Validation complete, detected %d errors out of %d validations." % (total_validations_failed, total_validation_count))
    if SEND_EMAIL_ALERTS:
        logger.info("Sending email alert.")
        email_message = "Validation complete, detected %d errors out of %d validations." % (total_validations_failed, total_validation_count)
        if INCLUDE_VERBOSE_REPORT_AS_ATTACHMENT:
            send_report_with_attachment(email_message)
        else:
            send_report(email_message)

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

def send_report_with_attachment(message):
    response = client.send_raw_email(
        Destination={
            'ToAddresses': [
                DESTINATION_EMAIL,
            ],
            'CcAddresses': [
            ],
            'BccAddresses': [
            ],
        },
        FromArn='',
        RawMessage={
            'Data': 'From: sender@example.com\nTo: recipient@example.com\nSubject: Test email (contains an attachment)\nMIME-Version: 1.0\nContent-type: Multipart/Mixed; boundary="NextPart"\n\n--NextPart\nContent-Type: text/plain\n\nThis is the message body.\n\n--NextPart\nContent-Type: text/plain;\nContent-Disposition: attachment; filename="attachment.txt"\n\nThis is the text in the attachment.\n\n--NextPart--',
        },
        ReturnPathArn='',
        Source='',
        SourceArn='',
    )

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
