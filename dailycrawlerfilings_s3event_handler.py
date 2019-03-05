import boto3, json

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    invokeLam = boto3.client("lambda", region_name ="eu-west-1")
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    payload = json.loads(obj['Body'].read())
    res = invokeLam.invoke(FunctionName="", Event="Event", Payload = payload)
    return {
        'statusCode': 200,
        'body': 'Hello from Lambda!'
    }
