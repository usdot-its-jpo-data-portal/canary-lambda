import boto3
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import pkg_resources
import pickle
import queue
import uuid
from decimal import Decimal
from odevalidator import TestCase
from slacker import SlackMessage
from pysqs_extended_client.SQSClientExtended import SQSClientExtended

# Logger settings
VERBOSE_OUTPUT = True if os.environ.get('VERBOSE_OUTPUT').upper() == 'TRUE' else False

### Set this variable to FALSE to deactivate (will switch to direct-query mode)
SQS_PUBLISHER_MODE = False if os.environ.get('SQS_PUBLISHER_MODE').upper() == 'FALSE' else True

if SQS_PUBLISHER_MODE:
    SQS_RESULT_QUEUE = os.environ.get('SQS_RESULT_QUEUE')
    assert SQS_RESULT_QUEUE != None, "Failed to get SQS_RESULT_QUEUE from environment (required for SQS_PUBLISHER_MODE)"
    SQS_STORAGE_S3_BUCKET = os.environ.get('SQS_STORAGE_S3_BUCKET')
    assert SQS_STORAGE_S3_BUCKET != None, "Failed to get SQS_STORAGE_S3_BUCKET from environment (required for SQS_PUBLISHER_MODE)"

else:
    ### Data source settings
    S3_BUCKET = os.environ.get('S3_BUCKET')
    DATA_PROVIDERS = os.environ.get('DATA_PROVIDERS').split(',')
    MESSAGE_TYPES = os.environ.get('MESSAGE_TYPES').split(',')

    ### Alerting settings
    SEND_SLACK_MESSAGE = True if os.environ.get('SEND_SLACK_MESSAGE') == 'TRUE' else False
    SLACK_WEBHOOK = os.environ.get('SLACK_WEBHOOK')

    ### Debugging settings
    USE_STATIC_PREFIXES = True if os.environ.get('USE_STATIC_PREFIXES') == 'TRUE' else False
    STATIC_PREFIXES = os.environ.get('STATIC_PREFIXES').split(',')
    DAY_OFFSET = int(os.environ.get('DAY_OFFSET'))

    ### Local testing settings
    LOCAL_TEST_FILE = "test/data.txt"

VALIDATING_PILOTS = [('wydot', 'bsm'), ('wydot', 'tim'), ('thea', 'bsm'), ('thea', 'tim'), ('thea', 'spat')]

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

def lambda_handler(event, context):
    if SQS_PUBLISHER_MODE:
        sqs_validate(event=event, context=context)
    else:
        validate(local_test=False, context=context)

def sqs_validate(event, context):

    sqs_extended = SQSClientExtended(s3_bucket_name=SQS_STORAGE_S3_BUCKET)

    logger.debug("Received SQS event: %s" % event)

    s3_client = boto3.client('s3')
    sqs_client = boto3.client('sqs')
    results_queue = boto3.resource('sqs').get_queue_by_name(QueueName=SQS_RESULT_QUEUE)
    test_case_dict = {'{}_{}'.format(pilot, messageType):
        TestCase(pkg_resources.resource_filename('odevalidator', 'configs/config_{}_{}.ini'.format(pilot, messageType)))
        for pilot, messageType in VALIDATING_PILOTS
    }

    logger.info("SQS event received. Number of records in SQS event: %d" % len(event['Records']))

    for sqs_message in event['Records']:
        # Validate records from file
        sqs_message_body = json.loads(sqs_message['body'])
        bucket = sqs_message_body['bucket']
        file_key = sqs_message_body['key']
        pilot_name = sqs_message_body['pilot_name']
        message_type = sqs_message_body['message_type']
        test_case_key = '{}_{}'.format(pilot_name.lower(), message_type.lower())

        logger.info("Processing data file with path: %s/%s" % (bucket, file_key))
        record_list = extract_records_from_file(s3_client, file_key, bucket, False)
        logger.debug("Found %d records in file." % len(record_list))

        shortened_results = {
            'errors': [],
            'num_validations': 0,
            'num_validation_errors': 0,
            'num_records': len(record_list),
            'num_records_w_errors': 0
        }
        if test_case_key in test_case_dict:
            test_case = test_case_dict[test_case_key]
            msg_queue = queue.Queue()
            for record in record_list:
                msg_queue.put(str(record, 'utf-8'))
            validation_results = test_case.validate_queue(msg_queue)

            # summarize validation results
            for result in validation_results:
                result = result.to_json()
                error = ['{}: {}: {}'.format(result['SerialId'], i['Field'], i['Details']) for i in result['Validations'] if not i['Valid']]
                shortened_results['errors'] += error
                shortened_results['num_validations'] += len(result['Validations'])
                shortened_results['num_validation_errors'] += len(error)
                if error:
                    shortened_results['num_records_w_errors'] += 1
                # TODO: keep track of record associated with invalid validation results in the future

        # Send off results
        msg = {
            'key': "%s/%s" % (bucket, file_key),
            'results': shortened_results,
            'data_group': '{}:{}'.format(pilot_name, message_type)
        }
        logger.debug("Publishing results to queue with MessageGroupId = %s." % SQS_RESULT_QUEUE)

        logger.debug("Querying for URL of result queue...")
        result_queue_url = sqs_client.get_queue_url(QueueName=SQS_RESULT_QUEUE)['QueueUrl']
        logger.debug("Found results queue URL from query: %s" % result_queue_url)

        logger.debug("Publishing results to SQS queue...")
        sqs_extended.send_message(
            queue_url=result_queue_url,
            message_body=json.dumps(msg),
            message_group_id=SQS_RESULT_QUEUE,
            message_deduplication_id=str(uuid.uuid4()),
            message_attributes={}
        )
        logger.info("Validation results successfully published to results SQS queue.")

        # Delete file message from queue
        receipt_handle = sqs_message['receiptHandle']
        queue_name = sqs_message['eventSourceARN'].split(':')[5]
        logger.debug("Querying for queue_url for message deletion. MessageReceiptHandle: %s, QueueName: %s" % (receipt_handle, queue_name))
        queue_url = sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
        logger.debug("Received queue_url from query request: %s" % (queue_url))
        logger.debug("Sending deletion request for ReceiptHandle %s to QueueUrl %s" % (receipt_handle, queue_url))
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        logger.info("Message successfully deleted from ingest SQS queue.")

def validate(local_test, context):
    function_start_time = datetime.now()

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
        record_list = extract_records_from_file(s3_client, filename, S3_BUCKET, local_test)
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
def extract_records_from_file(s3_client, filename, s3_bucket, local_test):
    if local_test:
        print("(Local test) Loading test data from local file.")
        test_records = []
        with open(LOCAL_TEST_FILE) as test_file:
            for line in test_file:
                test_records.append(line)
        return test_records
    else:
        s3_file = s3_client.get_object(
            Bucket=s3_bucket,
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
    if SQS_PUBLISHER_MODE:
        test_event = {
          "Records": [
            {
              "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
              "receiptHandle": "MessageReceiptHandle",
              "body": "{\"bucket\": \"test-usdot-its-cvpilot-public-data\", \"key\": \"wydot/BSM/2018/12/14/23/test-usdot-its-cvpilot-bsm-public-0-2018-12-14-23-00-00-2b7bc4ce-b93c-40a6-bdcb-7a1b02e1d9da\"}",
              "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1523232000000",
                "SenderId": "123456789012",
                "ApproximateFirstReceiveTimestamp": "1523232000001"
              },
              "messageAttributes": {},
              "md5OfBody": "7b270e59b47ff90a553787216d55d91d",
              "eventSource": "aws:sqs",
              "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
              "awsRegion": "us-east-1"
            }
          ]
        }
        sqs_validate(event=test_event, context=None)
    else:
        validate(local_test=True, context=None)
