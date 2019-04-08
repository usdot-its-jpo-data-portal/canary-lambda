import boto3
import datetime
import json
import queue
import logging
from decimal import Decimal
from odevalidator import TestCase
from emailer import Emailer

### Data source configuration settings
USE_STATIC_PREFIX = False
STATIC_PREFIX = "wydot/BSM/2019/04"

### Data source config
S3_BUCKET = "usdot-its-cvpilot-public-data"
DATA_PROVIDER = "wydot"
MESSAGE_TYPE = "BSM"
SAMPLE_SIZE = 10

### Outbound email alert settings
SEND_EMAIL_ALERTS = False
INCLUDE_VERBOSE_REPORT_AS_ATTACHMENT = True
SOURCE_EMAIL = "sender@email.com"
DESTINATION_EMAIL = "receiver@email.com"

### For local testing
LOCAL_TEST_FILE = "test/data.txt"

def lambda_handler(event, context):
    validate(False)

def validate(local_test):
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
    test_case = TestCase()

    prefix_string = ""
    if USE_STATIC_PREFIX:
        prefix_string = STATIC_PREFIX
    else:
        ddate = datetime.datetime.now()
        prefix_string = "%s/%s/%s/%s/%s" % (DATA_PROVIDER, MESSAGE_TYPE, ddate.year, str(ddate.month).zfill(2), str(ddate.day).zfill(2))

    s3_file_list = list_s3_files_matching_prefix(local_test, prefix_string)
    logger.info("Queried for %d S3 files matching prefix string '%s'. Found %d matching files." % (SAMPLE_SIZE, prefix_string, len(s3_file_list)))
    logger.info("Matching files: [%s]" % ", ".join(s3_file_list))

    log_file_list = []
    total_validation_count = 0
    total_validations_failed = 0
    msg_queue = queue.Queue()
    for filename in s3_file_list:
        logger.info("Analyzing file '%s'" % filename)
        record_list = extract_records_from_file(local_test, filename)
        for record in record_list:
            msg_queue.put(record)
            log_file_name = json.loads(record)['metadata']['logFileName']


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
        emailer = Emailer()
        logger.info("Sending email alert.")
        email_message = "Validation complete, detected %d errors out of %d validations." % (total_validations_failed, total_validation_count)
        if INCLUDE_VERBOSE_REPORT_AS_ATTACHMENT:
            emailer.send_report_with_attachment(email_message)
        else:
            emailer.send_report(email_message)
    return



### Returns a list of records from a given file
def extract_records_from_file(local_test, filename):
    if local_test:
        print("(Local test) Loading test data from local file.")
        test_records = []
        with open(LOCAL_TEST_FILE) as test_file:
            for line in test_file:
                test_records.append(line)
        return test_records
    else:
        s3_file = boto3.client('s3').get_object(
            Bucket=S3_BUCKET,
            Key=filename,
        )
        return s3_file['Body'].read().splitlines()

### Returns filenames from an S3 list files (list_objects) query
def list_s3_files_matching_prefix(local_test, prefix_string):
    if local_test:
        print("(Local test) Skipping S3 file query.")
        return [LOCAL_TEST_FILE]
    else:
        response = boto3.client('s3').list_objects_v2(
            Bucket=S3_BUCKET,
            MaxKeys=SAMPLE_SIZE,
            Prefix=prefix_string,
        )
        filenames = []
        if response.get('Contents'):
            [filenames.append(item['Key']) for item in response.get('Contents')]
        return filenames

if __name__ == '__main__':
    print("(Local test) Running local test...")
    validate(True)
