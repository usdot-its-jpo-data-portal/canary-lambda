import boto3
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import queue
from decimal import Decimal
from odevalidator import TestCase
from slacker import SlackMessage

### Data source settings
S3_BUCKET = os.environ.get('S3_BUCKET')
DATA_PROVIDERS = os.environ.get('DATA_PROVIDERS').split(',')
MESSAGE_TYPES = os.environ.get('MESSAGE_TYPES').split(',')

### Alerting settings
SEND_SLACK_MESSAGE = True if os.environ.get('SEND_SLACK_MESSAGE') == 'TRUE' else False
SLACK_WEBHOOK = os.environ.get('SLACK_WEBHOOK')

### Debugging settings
VERBOSE_OUTPUT = True if os.environ.get('VERBOSE_OUTPUT') == 'TRUE' else False
USE_STATIC_PREFIXES = True if os.environ.get('USE_STATIC_PREFIXES') == 'TRUE' else False
STATIC_PREFIXES = os.environ.get('STATIC_PREFIXES').split(',')
DAY_OFFSET = int(os.environ.get('DAY_OFFSET'))

### Local testing settings
LOCAL_TEST_FILE = "test/data.txt"

def lambda_handler(event, context):
    validate(local_test=False, context=context)

def validate(local_test, context):
    function_start_time = datetime.now()
    # Setup logger
    root = logging.getLogger()
    if root.handlers: # Remove default AWS Lambda logging configuration
        for handler in root.handlers:
            root.removeHandler(handler)
    logger = logging.getLogger('canary')
    logging.basicConfig(format='%(levelname)s %(message)s')
    if VERBOSE_OUTPUT:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Setup S3 query params
    prefix_strings = []
    if USE_STATIC_PREFIXES:
        prefix_strings.extend(STATIC_PREFIXES)
    else:
        ddate = datetime.now(timezone.utc)+timedelta(days=DAY_OFFSET)
        for provider in DATA_PROVIDERS:
            for mtype in MESSAGE_TYPES:
                prefix_strings.append("%s/%s/%s/%s/%s" % (provider, mtype, ddate.year, str(ddate.month).zfill(2), str(ddate.day).zfill(2)))

    # Create a list of analyzable S3 files
    s3_client = boto3.client('s3')
    s3_file_list = []
    for prefix in prefix_strings:
        matched_file_list = list_s3_files_matching_prefix(s3_client, prefix, local_test)
        logger.debug("Queried for S3 files matching prefix string '%s'. Found %d matching files: [%s]" % (prefix, len(matched_file_list), ", ".join(matched_file_list)))
        s3_file_list.extend(matched_file_list)

    # Begin validation routine
    test_case = TestCase()

    log_file_list = []
    total_validation_count = 0
    total_validations_failed = 0
    records_analyzed = 0
    msg_queue = queue.Queue()
    error_list = []
    for filename in s3_file_list:
        logger.info("============================================================================")
        logger.info("Analyzing file '%s'" % filename)
        record_list = extract_records_from_file(s3_client, filename, local_test)
        for record in record_list:
            records_analyzed += 1
            if local_test:
                msg_queue.put(record)
            else:
                msg_queue.put(str(record, 'utf-8'))

        if msg_queue.qsize() == 0:
            logger.warning("Could not find any records to be validated in S3 file '%s'." % filename)
            logger.warning("============================================================================")
            continue

        validation_results = test_case.validate_queue(msg_queue)

        num_errors = 0
        num_validations = 0
        error_dict = {}
        for result in validation_results:
            num_validations += len(result.field_validations)
            for validation in result.field_validations:
                if validation.valid == False:
                    num_errors += 1
                    error_dict[json.dumps(validation.serial_id)] = validation.details

        total_validation_count += num_validations
        total_validations_failed += num_errors
        if num_errors > 0:
            logger.error("Validation has FAILED for file '%s'. Detected %d errors out of %d total validation checks." % (filename, num_errors, num_validations))
            if VERBOSE_OUTPUT:
                for error in error_dict:
                    error_list.append("SerialID: '%s', Error: '%s'" % (error, error_dict[error]))
                logger.info("\n".join(error_list))
            logger.error("============================================================================")
        else:
            logger.info("Validation has PASSED for file '%s'. Detected no errors out and performed %d total validation checks." % (filename, num_validations))
            logger.info("===========================================================================")

    logger.info("[CANARY FINISHED] Validation complete, detected %d errors out of %d validations." % (total_validations_failed, total_validation_count))
    if SEND_SLACK_MESSAGE:
        slack_message = SlackMessage(
            success = total_validations_failed == 0,
            prefixes = prefix_strings,
            filecount = len(s3_file_list),
            recordcount = records_analyzed,
            validationcount = total_validation_count,
            errorcount = total_validations_failed,
            errorstring = "\n\n".join(error_list),
            starttime = function_start_time,
            endtime = datetime.now(),
            function_name = context.function_name,
            aws_request_id = context.aws_request_id,
            log_group_name = context.log_group_name,
            log_stream_name = context.log_stream_name,
        )
        slack_message.send(logger, SLACK_WEBHOOK)
    return

### Returns a list of records from a given file
def extract_records_from_file(s3_client, filename, local_test):
    if local_test:
        print("(Local test) Loading test data from local file.")
        test_records = []
        with open(LOCAL_TEST_FILE) as test_file:
            for line in test_file:
                test_records.append(line)
        return test_records
    else:
        s3_file = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key=filename,
        )
        return s3_file['Body'].read().splitlines()

### Returns filenames from an S3 list files (list_objects) query
def list_s3_files_matching_prefix(s3_client, prefix_string, local_test):
    if local_test:
        print("(Local test) Skipping S3 file query.")
        return [LOCAL_TEST_FILE]
    else:
        response = list_s3_objects(s3_client, prefix_string)
        filenames = []
        if response.get('Contents'):
            [filenames.append(item['Key']) for item in response.get('Contents')]
        while response.get('NextContinuationToken'):
            response = list_s3_objects(s3_client, prefix_string, response.get('NextContinuationToken'))
            if response.get('Contents'):
                [filenames.append(item['Key']) for item in response.get('Contents')]
        return filenames

def list_s3_objects(s3_client, prefix_string, continuation_token=None):
    if continuation_token:
        return s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix_string,
            ContinuationToken=continuation_token,
        )
    else:
        return s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix_string,
        )

if __name__ == '__main__':
    print("(Local test) Running local test...")
    validate(local_test=True, context=None)
