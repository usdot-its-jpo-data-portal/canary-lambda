import boto3
import datetime
import json
from decimal import Decimal

### Data source config
S3_BUCKET = "usdot-its-cvpilot-public-data"
DATA_PROVIDER = "wydot"
MESSAGE_TYPE = "BSM"
SAMPLE_SIZE = 1000

### Geofence config (Wyoming)
FENCE_UPPER_LAT = Decimal(45.00)
FENCE_LOWER_LAT = Decimal(41.00)
FENCE_UPPER_LON = Decimal(-104.00)
FENCE_LOWER_LON = Decimal(-111.00)

### Timefence config (messages inside this range are considered invalid)
EARLIEST_DATETIME = datetime.date(2019, 2, 12)
LATEST_DATETIME = datetime.date(2018, 12, 3)

### Operational config
PRINT_LOG_FILE_NAMES = False
VALIDATE_SERIAL_NUMBERS = False
VALIDATE_RECORD_GENERATED_AT = False

def lambda_handler(event, context):

    s3_client = boto3.client('s3')

    # ddate = datetime.datetime.now()
    # dyear = ddate.year
    # dmonth = str(ddate.month).zfill(2)
    # dday = str(ddate.day).zfill(2)
    # prefix_string = "%s/%s/%s/%s/%s" % (DATA_PROVIDER, MESSAGE_TYPE, dyear, dmonth, dday)

    prefix_string = "wydot/BSM/2018/12"

    print("Searching for files with prefix <%s>" % prefix_string)

    total_record_count = count_total_records(s3_client, prefix_string)
    print("Found %d records in the timeframe." % total_record_count)

    if PRINT_LOG_FILE_NAMES:
        print_log_files_names(all_records)

    if VALIDATE_SERIAL_NUMBERS:
        check_serial_numbers(all_records)

    if VALIDATE_RECORD_GENERATED_AT:
        check_recordGeneratedAt(all_records)



def count_total_records(s3_client, prefix_string):

    file_list = list_s3_files_matching_prefix(s3_client, prefix_string)

    if len(file_list) == 0:
        return 0

    total_record_count = count_total_records_in_timerange(s3_client, file_list, EARLIEST_DATETIME, LATEST_DATETIME)

    return total_record_count

###
def list_s3_files_matching_prefix(s3_client, prefix_string):
    query_response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET,
        MaxKeys=SAMPLE_SIZE,
        Prefix=prefix_string,
    )
    # return query_response['Contents']
    # print(json.dumps(query_response))
    print(query_response)
    return query_response.get('Contents')

### Iterate list of s3 filenames and count the number of records whose metadata.odeReceivedAt falls between two datetimes
def count_total_records_in_timerange(s3_client, s3_file_list, start_datetime, end_datetime):
    total_record_count = 0
    for filename in s3_file_list:
        s3_file = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key=filename['Key'],
        )
        for record in s3_file['Body'].read().splitlines():
            try:
                record_generated_at = datetime.datetime.fromisoformat(json.loads(record)['metadata']['recordGeneratedAt'])
                if record_generated_at > EARLIEST_DATETIME and record_generated_at < LATEST_DATETIME:
                    total_record_count += 1
                else:
                    print("Detected message with recordGeneratedAt outside of timefence: %s" % record)
            except Exception as e:
                print("WARNING! Unable to extract recordGeneratedAt from record. Error: %s, S3 filename: %s, record: %s" % (str(e), filename, record))
    return total_record_count

def s3_select_query(s3_client, filename, query_string):
    #query_string = "SELECT COUNT(*) WHERE metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt < dateOfBugFix"
    #query_string = "SELECT * FROM S3Object[*].metadata.odeReceivedAt"

    response = s3_client.select_object_content(
    Bucket=S3_BUCKET,
    Key=filename,
    Expression=query_string,
    ExpressionType='SQL',
    InputSerialization={
        'CompressionType': 'NONE',
        'JSON': {
            'Type': 'LINES'
        }

    },
    OutputSerialization={
        'JSON': {
            'RecordDelimiter': '\n'
        }
    }
)

### Check coordinates are inside (or on) fence
def check_geofence(lat, lon):
    if lat > FENCE_UPPER_LAT or lat < FENCE_LOWER_LAT:
        return False
    if lon > FENCE_UPPER_LON or lon < FENCE_LOWER_LON:
        return False
    return True

### Check for identical serial numbers
def check_serial_numbers(record_list):
    old_serial_number = "-1"
    num_duplicates = 0
    for record in record_list:
        new_serial_number = json.loads(record)['metadata']['serialId']['serialNumber']
        if old_serial_number == new_serial_number:
            num_duplicates = num_duplicates + 1
            print("WARNING! Duplicate serial number detected: %s" % old_serial_number)
            print("WARNING! Number of duplicates detected: %d of %d records" % (num_duplicates, len(record_list)))
        old_serial_number = new_serial_number

### Check for identical recordGeneratedAt numbers
def check_recordGeneratedAt(record_list):
    old_recordGeneratedAt = "timestamp"
    num_duplicates = 0
    for record in record_list:
        json_record = json.loads(record)
        new_recordGeneratedAt = json_record['metadata']['recordGeneratedAt']
        filename = json_record['metadata']['logFileName']
        if old_recordGeneratedAt == new_recordGeneratedAt:
            num_duplicates = num_duplicates + 1
            #print("WARNING! Duplicate recordGeneratedAt detected: [%s], log file: [%s]" % (old_recordGeneratedAt, filename))
        old_recordGeneratedAt = new_recordGeneratedAt
    print("WARNING! Number of duplicates detected: %d of %d records" % (num_duplicates, len(record_list)))

### Print how many log files there were and how many messages per file
def print_log_files_names(record_list):
    filenames = {}
    for record in record_list:
        filename = json.loads(record)['metadata']['logFileName']
        if filename in filenames:
            filenames[filename] = filenames[filename] + 1
        else:
            filenames[filename] = 1
    print("Unique log files: %d" % len(filenames))
    print("-Filename, Entries-")
    for f in filenames:
        print("%s, %d" % (f, filenames[f]))

### Print record types
def print_record_types(record_list):
    record_types = {}
    for record in record_list:
        record_type = json.loads(record)['metadata']['recordType']
        if record_type in record_types:
            record_types[record_type] = record_types[record_type] + 1
        else:
            record_types[record_type] = 1
    print("Unique record types: %d" % len(record_types))
    print("-Record Type, Occurrences-")
    for r in record_types:
        print("%s, %d" % (r, record_types[r]))
