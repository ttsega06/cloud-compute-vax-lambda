import boto3
import json
import os
from botocore.exceptions import ClientError

sqs_client = boto3.client('sqs')


def lambda_handler(event, context):
    sqs_queue_url = 'https://sqs.eu-north-1.amazonaws.com/057745697967/project3upqueue'
    email_subject = 'Violation Report'

    # Retrieve messages from the SQS queue
    records = event['Records']
    for record in records:
        receipt_handle = record['receiptHandle']
        message_body = record['body']
        print(f"Received message: {message_body}")

        message_parts = message_body.split(', ')
        message_dict = {}
        for part in message_parts:
            if ': ' in part:
                # Split at the first ": " only
                key, value = part.split(': ', 1)
                message_dict[key.strip()] = value.strip()

        plate_number = message_dict.get('Plate Number', '')
        model = message_dict.get('Model', '')
        make = message_dict.get('Make', '')
        location = message_dict.get('Location', '')
        date_time = message_dict.get('DateTime', '')
        violation_type = message_dict.get('Type', '')
        color = message_dict.get('Color', '')
        owner_name = message_dict.get('Owner Name', '')
        owner_contact = message_dict.get('Contact', '')
        ticket_amount = calculate_ticket_amount(violation_type)

        # Compose the email message
        email_body = f"Vehicle: {color} {make} {model}\n"
        email_body += f"License plate: {plate_number}\n"
        email_body += f"Date: {date_time}\n"
        email_body += f"Violation address: {location}\n"
        email_body += f"Violation type: {violation_type}\n"
        email_body += f"Ticket amount: ${ticket_amount:.2f}"
        # Send the email
        send_email(owner_contact, email_subject, email_body)
        print(email_body)

        # Delete the processed message from the SQS queue
        sqs_client.delete_message(
            QueueUrl=sqs_queue_url,
            ReceiptHandle=receipt_handle
        )

    return {
        'statusCode': 200,
        'body': 'Email sent successfully'
    }


def calculate_ticket_amount(violation_type):
    ticket_amounts = {
        'no_stop': 300.00,
        'no_full_stop_on_right': 75.00,
        'no_right_on_red': 125.00
    }
    return ticket_amounts.get(violation_type, 0.00)


def send_email(to_address, subject, body):
    ses_client = boto3.client('ses', region_name='us-west-2')
    sender_email = 'mesteddy14@gmail.com'

    try:
        response = ses_client.send_email(
            Destination={
                'ToAddresses': [to_address]
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': body
                    }
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': subject
                }
            },
            Source=sender_email
        )
    except ClientError as e:
        print(f"Failed to send email: {e.response['Error']['Message']}")
        raise e
