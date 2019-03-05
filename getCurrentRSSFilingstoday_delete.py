import json, boto3

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    bucket = 'maks-alexandria-data'
    file_name = 'SEC/getCurrentRSS/Filingstoday.json'
    s3.put_object(Bucket = bucket, Key=file_name, Body= json.dumps([]))
    return {
        'statusCode': 200,
        'body': 'Successfully deleted dailyfilings json..'
    }
