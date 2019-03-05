import boto3, json

ses = boto3.client('ses')
email_from = 'Alexandria <phanisarma.nagavarapu@moodys.com>'

def lambda_handler(event, context):
    try:
        email_to = event['to']
        #print obj['to']
        name = event['companyname']
        filingtype = event['formtype']
        htmltable = event['table']
        response = ses.send_templated_email(
            Source = email_from,
            Template = 'FilingsTemplate',
            Destination={'ToAddresses': email_to},
            TemplateData = '{\"companyname\":\"'+name+'\", \
                            \"formtype\":\"'+filingtype+'\", \
                            \"table\":\"'+htmltable+'\"}'
        )
    except Exception as e:
        print e
