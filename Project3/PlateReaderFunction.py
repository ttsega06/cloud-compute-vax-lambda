import boto3
import json

rekognition_client = boto3.client(
    'rekognition', region_name='us-west-2', endpoint_url='https://rekognition.us-west-2.amazonaws.com')
sqs_client = boto3.client('sqs')
s3 = boto3.client('s3')
event_bridge = boto3.client('events')
queue_url = 'https://sqs.eu-north-1.amazonaws.com/057745697967/project3downqueue'


def lambda_handler(event, context):
    s3_event_record = event['Records'][0]['s3']

    bucket_name = s3_event_record['bucket']['name']
    photo = s3_event_record['object']['key']
    image_bytes = get_image_bytes(photo, bucket_name)

    metadata = retrieve_object_metadata(bucket_name, photo, s3)
    Plate = detect_text(image_bytes)
    location = metadata.get('location')
    date_time = metadata.get('datetime')
    violation_type = metadata.get('type')

    print(metadata)
    message_body = {
        'Plate': Plate,
        'Location': location,
        'DateTime': date_time,
        'Type': violation_type
    }
    # Write the message to SQS
    if len(Plate) == 7:
        sqs_client.send_message(
            QueueUrl=queue_url, MessageBody=json.dumps(message_body))
    else:
        message_body = {
            'Photo': photo,
            'Location': location,
            'DateTime': date_time,
            'Type': violation_type
        }
        event_bus_name = 'arn:aws:events:eu-north-1:057745697967:event-bus/default'
        event_source = 'Plates out of state'
        detail_type = 'violation'
        detail = message_body
        send_event(event_bus_name, event_source, detail_type, detail)

    response = {
        'statusCode': 200,
        'body': json.dumps({
            'bucket_name': bucket_name,
            'Plate': Plate
        })
    }

    return response


def get_image_bytes(photo, bucket):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=photo)
    image_bytes = response['Body'].read()
    return image_bytes


def detect_text(image_bytes):
    try:
        response = rekognition_client.detect_text(Image={'Bytes': image_bytes})
        text_detections = response['TextDetections']

    except Exception as e:
        print('An error occurred: {}'.format(e))
        return 0
    plate = " "
    for text in text_detections:

        if (IsCapitalLettersAndNumbers(str(text['DetectedText']))):
            plate = text['DetectedText']
            print('Detected text: ' + text['DetectedText'])

    return plate


def IsCapitalLettersAndNumbers(string):

    count = 0
    for char in string:
        if char.isnumeric():
            count += 1

    if count > 6:
        return False

    for char in string:
        if char.islower() or char == " ":
            return False

    return True


def retrieve_object_metadata(bucket_name, object_key, s3_client):
    response2 = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    metadata = response2.get('Metadata', {})
    return metadata


def send_event(event_bus_name, event_source, detail_type, detail):

    response = event_bridge.put_events(
        Entries=[
            {
                'Source': event_source,
                'DetailType': detail_type,
                'Detail': json.dumps(detail),
                'EventBusName': event_bus_name
            }
        ]
    )

    if response['FailedEntryCount'] > 0:
        print(f"Failed to send event: {response['Entries'][0]['ErrorCode']}")
    else:
        print("Event sent successfully")
