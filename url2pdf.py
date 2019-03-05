import boto3
import botocore
import pdfkit
import os
import time

# Checks if the file exists
time_str = str(time.strftime("%Y%m%d-%H%M%S"))


def custom_exception(e):
    exception_type = e.__class__.__name__
    exception_message = e.message
    response = dict()

    api_exception_obj = {
        "type": exception_type,
        "description": exception_message
    }
    response['file_url'] = ""
    response['code'] = 400
    response['message'] = api_exception_obj
    return response


def put_to_s3(url, filename, bucket, option):
    response = dict()
    client = boto3.client('s3')

    try:
        #pdf_file = "%s_%s.pdf" % (filename, time_str)
        #pdf_folder = "SEC/FinancialStatements/PDF/"
        #key = pdf_folder + pdf_file
        key = filename
    
        # create the temproary file path
        tempfilepath = os.path.join("/tmp/" + filename)
    
        # Create boto3 Object to destination bucket
        config = pdfkit.configuration(wkhtmltopdf='/wkhtmltox/bin/wkhtmltopdf')
        pdfkit.from_url(url, tempfilepath , options=option, configuration=config)
        #s3destination = boto3.resource('s3')
        data = open(tempfilepath, 'rb')
        #s3destination.Bucket(bucket).put_object_acl(Key=key, Body=data, ACL= 'public-read')
        client.put_object(Body = data, Bucket =bucket, Key=key)
        os.remove(tempfilepath)
        file_url = '%s/%s/%s' % (client.meta.endpoint_url, bucket, key)

    except Exception as e:
        api_exception_json = custom_exception(e)
        return api_exception_json

    response['file_url'] = file_url
    response['code'] = 200
    response['message'] = "success"
    return response


# Main function. Entrypoint for Lambda
def lambda_handler(event, context):
    url = event['formurl']
    options = event['options']
    Ticker = event['ticker']
    date = event['date']
    formtype = event['formtype']
    filename = "%s-%s-%s.pdf" % (Ticker,formtype, date)
    bucket = "maks-alexandria-financialstatements-pdf"
    response = put_to_s3(url, filename, bucket, options)
    return response
