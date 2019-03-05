import time
import boto3
import json


class QueryAthena:
    def __init__(self):
        self.s3_input = 's3://aws-athena-query-results-379444605927-eu-west-1/Unsaved'
        self.s3_output = 's3://aws-athena-query-results-379444605927-eu-west-1/Unsaved'
        self.database = 'alexandria_staged'
        self.table = ''
        self.region_name = 'eu-west-1'

    def load_conf(self, q):
        self.client = boto3.client('athena', region_name=self.region_name)

        try:
            response = self.client.start_query_execution(
                QueryString=q,
                QueryExecutionContext={
                    'Database': self.database
                },
                ResultConfiguration={
                    'OutputLocation': self.s3_output
                })
            return response
            # print('Execution ID: ' + response['QueryExecutionId'])
        except Exception as e:
            print(e)
            return None

    def query(self):
        self.query = "SELECT distinct cik, company_common_name, exchange_ticker FROM alexandria_staged.reference_master where exchange_ticker <>'' and company_common_name <>'' order by company_common_name;"

    def run_query(self):
        self.query()
        queries = [self.query]
        for q in queries:
            # print("Executing query: %s" % (q))
            res = self.load_conf(q)
            # print q
        if res is not None:
            try:
                query_status = None
                while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                    query_status = self.client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution']['Status']['State']
                    if query_status == 'FAILED' or query_status == 'CANCELLED':
                        raise Exception('Athena query failed or was cancelled')
                    time.sleep(0.5)
                # print("Query %s finished." % (self.endpoint))
    
                response = self.client.get_query_results(QueryExecutionId=res['QueryExecutionId'])
                res = json.loads(json.dumps(response))
                results = []
                data_list = []
                res_lst = []
                for row in res['ResultSet']['Rows']:
                    data_list.append(row['Data'])
                for datum in data_list[1:]:
                    results.append([x['VarCharValue'] for x in datum])
                for b in results:
                    dict = {
                        'cik': b[0],
                        'companyname': b[1],
                        'ticker': b[2],
                    }
                    res_lst.append(dict)
                return {
                    'statusCode': 200,
                    'isError': False,
                    'body': res_lst
                }
            except Exception as e:
                exception_type = e.__class__.__name__
                exception_message = e.message
                api_exceptoion_obj = {
                    'statusCode': 400,
                    'isError': True,
                    'type': exception_type,
                    'body': exception_message
                }
                return api_exceptoion_obj
        else:
            excep_obj = {
                'statusCode': 400,
                'isError': True,
                'type': 'ClientException',
                'body': 'Athena client error occurred !!'
            }
            return excep_obj
    # def func(end):
    #     qa = QueryAthena(end, "2018-01-01", "2018-01-31")
    #     result = qa.run_query()
    #     return result
    #
    # endpoints = ["677SRI149821", "V14509674", "1426R"]
    #
    # if __name__ == '__main__':
    #     pool = Pool(15)
    #     df = pd.concat(pool.map(func, endpoints))
    #     pool.close()
    #     pool.join()


def lambda_handler(event, context):
    qa = QueryAthena()
    result = qa.run_query()
    return result
