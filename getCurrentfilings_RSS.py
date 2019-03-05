import feedparser
from datetime import datetime
from time import mktime
# import ssl
import boto3
import re
import json
import requests
from bs4 import BeautifulSoup
import concurrent.futures

url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&owner=exclude'

print ('monitoring feed...')
FILER_TITLE_RE = re.compile(r'(.*) - (.*) \((.*)\) \((.*)\)')


def save_file_to_s3(bucket, file_name, data):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=file_name, Body=json.dumps(data))


def landing_url(url):
    response = requests.get(url)
    if response.status_code in (200, 201, 202):
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find("table", {"class": "tableFile", "summary": "Document Format Files"})
        links = tables.find_all('a')
        for l in links:
            if l.get_text() != '':
                form_url = 'https://www.sec.gov' + l.get('href')
                break
        return form_url
    else:
        return None


def func(dict):
    formurl = landing_url(dict['inter_url'])
    dict.update({'formurl': formurl})
    return dict


def concurrenctfunc(data):
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(func, dict): dict for dict in data}
        for future in concurrent.futures.as_completed(future_to_url):
            url_new = future_to_url[future]
            try:
                res.append(future.result())
            except Exception as exc:
                print('%r generated an exception: %s' % (url_new, exc))
    return res


def RSSscrape(start, cik, type):
    feed_url = url + '&start=' + str(start) + '&CIK=' + str(cik) + '&type=' + str(type) + '&count=10&output=atom'
    data = []
    full_data = feedparser.parse(feed_url)
    if full_data["entries"].__len__() != 0:
        for entry in full_data["entries"]:
            try:
                link = entry["link"]
                title_entries = FILER_TITLE_RE.findall(entry["title"])
                form_type = title_entries[0][0]
                company_name = title_entries[0][1]
                cik = title_entries[0][2]
                # acc_number = entry["id"].split("accession-number=")[1]
                # form_url = landing_url(entry["link"])
                date = str(datetime.fromtimestamp(mktime(entry["updated_parsed"])))

                form_dict = {
                    "companyname": company_name,
                    "formtype": form_type,
                    "cik": cik,
                    "date": date,
                    "inter_url": link
                    # "formurl": form_url
                    # "Raw Title"    : entry["title"],
                    # "AccessionNumber": acc_number
                }
                data.append(form_dict)
            except Exception, e:
                pass
    return data


def lambda_handler(event, context):
    sqs_queue = 'https://sqs.eu-west-1.amazonaws.com/379444605927/edgar_daily_url'
    _8k_queue = 'https://sqs.eu-west-1.amazonaws.com/379444605927/SEC_8K_queue'
    _10k_queue = 'https://sqs.eu-west-1.amazonaws.com/379444605927/SEC_10K_queue'
    bucket = 'maks-alexandria-data'
    file_name = 'SEC/getCurrentRSS/Filingstoday.json'
    s3 = boto3.client('s3')
    try:
        old = s3.get_object(Bucket=bucket, Key=file_name)
    except:
        s3.put_object(Bucket=bucket, Key=file_name, Body=json.dumps([]))

    old = s3.get_object(Bucket=bucket, Key=file_name)
    old = json.loads(old['Body'].read())
    updated_old = list(old)
    updated_new = []
    sqs = boto3.client('sqs')

    try:
        new = json.loads(json.dumps(RSSscrape('', '', '8-K')))
        for m in new:
            if m not in old:
                updated_new.append(m)
                updated_old.append(m)
        if old != updated_old:
            save_file_to_s3(bucket, file_name, updated_old)

        if updated_new.__len__() != 0:
            updated_new_f = concurrenctfunc(updated_new)
            _8k_data = filter(lambda updated_new_f: updated_new_f['formtype'] == '8-K', updated_new_f)
            _10k_data = filter(lambda updated_new_f: updated_new_f['formtype'] == '10-K', updated_new_f)
            # print _8k_data
            # sqs.send_message(
            #     QueueUrl= sqs_queue,
            #     DelaySeconds=0,
            #     MessageAttributes={
            #         'Title': {
            #             'DataType': 'String',
            #             'StringValue': 'Latest 100 filings from SEC RSS (All)'
            #         }
            #     },
            #     MessageBody= json.dumps(updated_new_f))
            if _8k_data.__len__() != 0:
                sqs.send_message(
                    QueueUrl=_8k_queue,
                    DelaySeconds=0,
                    MessageAttributes={
                        'Title': {
                            'DataType': 'String',
                            'StringValue': '8-K filings from SEC RSS'
                        }
                    },
                    MessageBody=json.dumps(_8k_data))
            if _10k_data.__len__() != 0:
                sqs.send_message(
                    QueueUrl=_10k_queue,
                    DelaySeconds=0,
                    MessageAttributes={
                        'Title': {
                            'DataType': 'String',
                            'StringValue': '10-K filings from SEC RSS'
                        }
                    },
                    MessageBody=json.dumps(_10k_data))
            return {
                'statusCode': 200,
                'isError': False,
                'body': 'Finished downloading for ' + str(updated_new_f.__len__())+ ' companies.'
            }
    except Exception as e:
        return {
            'statusCode': 400,
            'isError': False,
            'body': e.message
        }
