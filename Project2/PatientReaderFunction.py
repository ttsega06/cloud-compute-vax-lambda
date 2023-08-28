import boto3
import json
import xml.etree.ElementTree as ET

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
queue_url = 'https://sqs.eu-north-1.amazonaws.com/057745697967/project2downqueue'


def lambda_handler(event, context):

    try:
        # Retrieve the S3 bucket name and object key from the event
        s3_event_record = event['Records'][0]['s3']
        bucket_name = s3_event_record['bucket']['name']
        object_key = s3_event_record['object']['key']

        # Read the XML file content from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')

        # Parse the XML file and extract the data
        root = ET.fromstring(file_content)
        patient_id = root.find('id').text
        patient_name = root.find('name').text

        # Create a message body with the extracted data
        message_body = f'Patient ID: {patient_id}, Name: {patient_name}'

        # Write the message to SQS
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=message_body)

        return {
            'statusCode': 200,
            'body': 'XML file content has been successfully extracted and written to SQS.'
        }
    except Exception as e:

        return {

            'statusCode': 500,
            'body': f'An error occurred: {str(e)}'
        }
