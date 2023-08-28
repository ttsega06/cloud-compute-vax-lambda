import json
import boto3


def lambda_handler(event, context):
    records = event['Records']
    sqs = boto3.client('sqs')

    for record in records:
        body = record['body']
        receipt_handle = record['receiptHandle']
        queue_url = "https://sqs.eu-north-1.amazonaws.com/057745697967/project2upqueue"

        # Perform your deletion logic here
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

        print(body)
