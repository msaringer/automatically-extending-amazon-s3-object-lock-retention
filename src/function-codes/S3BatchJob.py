from urllib import parse
import boto3
import botocore
import os
import json
import logging
import datetime
import csv
import io
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Enable Verbose logging for Troubleshooting
# boto3.set_stream_logger("")

# Define Lambda Environmental Variable
my_role_arn = str(os.environ['batch_ops_role'])
report_bucket_name = str(os.environ['batch_ops_report_bucket'])
min_obj_retention = int(os.environ['obj_retention'])
retention_buffer = int(os.environ['retention_buffer'])
accountId = str(os.environ['my_account_id'])
my_region = str(os.environ['my_current_region'])
my_s3_bucket = str(os.environ['s3_bucket'])

## Construct Object Retention Date to Datet*ime
num_and_buffer = min_obj_retention + retention_buffer
my_min_obj_retention = datetime.datetime.now() + datetime.timedelta(num_and_buffer)


# Specify variables #############################
myexpression = "SELECT count(*) FROM s3object s"
my_csv_output_serialization = 'CSV'
my_fileheader_info = 'USE'

# Job Manifest Details ################################
job_manifest_format = 'S3BatchOperations_CSV_20180820'  # S3InventoryReport_CSV_20161130


# Job Report Details ############################
report_prefix = str(os.environ['batch_ops_restore_report_prefix'])
report_format = 'Report_CSV_20180820'
report_scope = 'AllTasks'

# Construct ARNs ############################################
report_bucket_arn = 'arn:aws:s3:::' + report_bucket_name

# Initiate Service Clients ###################
s3Client = boto3.client('s3', region_name=my_region)
s3ControlClient = boto3.client('s3control', region_name=my_region)

# S3 Select query to check the number of rows in the Athena CSV result.
# Ensure you include Scan range in query, do not scan the whole object. Large objects for example 65GB times out!
# Do not trigger S3 Batch if the number of rows is less than 1 (exluding the header)
#def select_query_function_csv(bucket, key, expression, fileheaderinfo, outputserialization):
#    resp = s3Client.select_object_content(
#        Bucket=bucket,
#        Key=key,
#        Expression=expression,
#        ExpressionType='SQL',
#        ScanRange={
#        'Start': 0,
#        'End': 10240
#        },
#        RequestProgress={
#            'Enabled': True
#        },
#        InputSerialization={
#            "CSV": {
#                'FileHeaderInfo': fileheaderinfo,
#            },
#        },
#        OutputSerialization={my_csv_output_serialization: {}}, )
#    for event in resp['Payload']:
#        if 'Records' in event:
#            # logger.info(event['Records']['Payload'].decode('utf-8'))
#            num_of_rows = int(event['Records']['Payload'].decode('utf-8'))
#            logger.info(f'There are {num_of_rows} of rows in the Athena query result')
#            return num_of_rows
#

# S3 Select is no longer supported, since we only need to know if there are more than 2 rows
# read the first 1MB of the .csv and determine if there are a non-zero number of rows
def select_query_function_csv(bucket, key, expression, fileheaderinfo, outputserialization):
    s3 = boto3.client('s3')
    
    # Try to get the first 1MB of the file
    try:
        response = s3.get_object(Bucket=bucket, Key=key, Range='bytes=0-1048575')
        content = response['Body'].read().decode('utf-8')
        is_truncated = int(response['ContentLength']) == 1048576  # Check if we got exactly 1MB
    except s3.exceptions.InvalidRange:
        # If the file is smaller than 1MB, read the entire file
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        is_truncated = False
    except Exception as e:
        logger.error(f"Error reading file {key} from bucket {bucket}: {str(e)}")
        return 0  # Return 0 if we can't read the file
    
    # Handle empty file
    if not content.strip():
        logger.info('The file is empty')
        return 0
    
    # Use StringIO to create a file-like object
    file_like_object = io.StringIO(content)
    
    # Create a CSV reader
    csv_reader = csv.reader(file_like_object)
    
    # Skip the header if necessary
    if fileheaderinfo == 'USE':
        try:
            next(csv_reader, None)
        except StopIteration:
            logger.info('The file only contains a header')
            return 0
    
    # Count all rows
    row_count = sum(1 for _ in csv_reader)
    
    if is_truncated:
        logger.info(f'There are at least {row_count} rows in the CSV file (file exceeds 1MB)')
    else:
        logger.info(f'There are exactly {row_count} rows in the CSV file')
    
    return row_count

# Retrieve Manifest ETag
def get_manifest_etag(manifest_s3_bucket, manifest_s3_key):
    # Get manifest key ETag ####################################
    try:
        manifest_key_object_etag = s3Client.head_object(Bucket=manifest_s3_bucket, Key=manifest_s3_key)['ETag']
    except ClientError as e:
        logger.error(e)
    else:
        logger.info(manifest_key_object_etag)
        return manifest_key_object_etag


# S3 Batch Restore Job Function

def s3_batch_ops_objlock(manifest_bucket, manifest_key, job_request_token):
    logger.info("Initiating the Amazon S3 Batch Operation Object Lock Retention Job")

    my_job_description = f"Auto Extend Object Lock Solution Job for S3Bucket: {my_s3_bucket}"

    # Construct ARNs ############################################
    manifest_bucket_arn = 'arn:aws:s3:::' + manifest_bucket
    manifest_key_arn = 'arn:aws:s3:::' + manifest_bucket + '/' + manifest_key
    # Get manifest key ETag ####################################
    manifest_key_object_etag = get_manifest_etag(manifest_bucket, manifest_key)

    # Set Manifest format and Specify Manifest Fields #
    manifest_format = None
    manifest_fields = None
    manifest_fields_count = None

    if "athena-query-results/" in manifest_key:
        logger.info("Set Format to CSV and Don't use Version ID in Manifest")
        manifest_format = 'S3BatchOperations_CSV_20180820'
        manifest_fields = ['Bucket', 'Key']
        manifest_fields_count = str(len(manifest_fields))


    my_bops_objlock_kwargs = {

        'AccountId': accountId,
        'ConfirmationRequired': False,
        'Operation': {
            'S3PutObjectRetention': {
                        'BypassGovernanceRetention': True,
                        'Retention': {
                            'RetainUntilDate': my_min_obj_retention,
                            'Mode': 'COMPLIANCE'
                        }
            }
        },
        'Report': {
            'Bucket': report_bucket_arn,
            'Format': report_format,
            'Enabled': True,
            'Prefix': report_prefix,
            'ReportScope': report_scope
        },
        'Manifest': {
            'Spec': {
                'Format': manifest_format,
                'Fields': manifest_fields
            },
            'Location': {
                'ObjectArn': manifest_key_arn,
                'ETag': manifest_key_object_etag
            }
        },
        'Description': my_job_description,
        'Priority': 10,
        'RoleArn': my_role_arn,
        'Tags': [
            {
                'Key': 'job-created-by',
                'Value': 'Auto Extend Object Lock Solution'
            },
        ],
        'ClientRequestToken': job_request_token
    }


    try:
        response = s3ControlClient.create_job(**my_bops_objlock_kwargs)
        logger.info(f"JobID is: {response['JobId']}")
        logger.info(f"S3 RequestID is: {response['ResponseMetadata']['RequestId']}")
        logger.info(f"S3 Extended RequestID is:{response['ResponseMetadata']['HostId']}")
        return response['JobId']
    except ClientError as e:
        logger.error(e)


def lambda_handler(event, context):
    logger.info(event)
    s3Bucket = str(event['Records'][0]['s3']['bucket']['name'])
    # Use sequencer to prevent duplicate invocation
    my_request_token = str(event['Records'][0]['s3']['object']['sequencer'])
    logger.info(s3Bucket)
    s3Key = parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    logger.info(s3Key)
    my_csv_num_rows = select_query_function_csv(s3Bucket, s3Key, myexpression, my_fileheader_info, my_csv_output_serialization)
    logger.info(my_csv_num_rows)
    if my_csv_num_rows > 1:
        logger.info(f'We have more than 1 row in our athena query result, now trigger the S3 Batch Job')
        job_id = s3_batch_ops_objlock(s3Bucket, s3Key, my_request_token)
    else:
        logger.info(f'No Action required, Athena returned no results')
