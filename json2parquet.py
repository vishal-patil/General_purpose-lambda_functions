import pyarrow as pa
from pyarrow import parquet as pq
from s3fs import S3FileSystem
from datetime import date, datetime, timedelta
import boto3
import json

def save_file_to_s3(bucket, file_name, data):
    s3 = S3FileSystem()
    table = pa.Table.from_batches([data])
    pq.write_to_dataset(table=table,
                        root_path='s3://' + bucket + '/' + file_name,
                        filesystem=s3)

def _convert_data_with_schema(data, schema, field_aliases=None):
    column_data = {}
    array_data = []
    schema_names = []
    for row in data:
        for column in schema.names:
            _col = column_data.get(column, [])
            _col.append(row.get(column))
            column_data[column] = _col
    for column in schema:
        _col = column_data.get(column.name)
        # Float types are ambiguous for conversions, need to specify the exact type
        if column.type.id == pa.float64().id:
            array_data.append(pa.array(_col, type=pa.float64()))
        elif column.type.id == pa.int32().id:
            # PyArrow 0.8.0 can cast int64 -> int32
            _col64 = pa.array(_col, type=pa.int64())
            array_data.append(_col64.cast(pa.int32()))
        else:
            array_data.append(pa.array(_col, type=column.type))
        if isinstance(field_aliases, dict):
            schema_names.append(field_aliases.get(column.name, column.name))
        else:
            schema_names.append(column.name)
    return pa.RecordBatch.from_arrays(array_data, schema_names)


def toparquet(res_obj):
    # schema = ["cik", "date","formtype","inter_url","companyname","formurl"]
    schema = pa.schema([
        pa.field("cik", pa.string()),
        pa.field("date", pa.string()),
        pa.field("formtype", pa.string()),
        pa.field("inter_url", pa.string()),
        pa.field("companyname", pa.string()),
        pa.field("formurl", pa.string())
    ])
    data = _convert_data_with_schema(res_obj, schema)
    return data


def lambda_handler(event, context):
    try:
        
        asofdate = date.today() - timedelta(1)
        #asofdate = date(2019,2,22)
        ymd = asofdate.strftime('%Y%m%d')
        s3 = boto3.client('s3')
        invokeLambda = boto3.client('lambda')
        bucket = 'maks-alexandria-data'
        prefix = 'SEC/FinancialStatements/dailycrawlerfilings/' + str(ymd) + '/'
        outputkey = 'SEC/EDGAR_dailycrawlerfilings/' + 'y=' + str(asofdate.year) + '/m=' + str(asofdate.month) + '/d=' + str(ymd)
        payload = []
        for obj in s3.list_objects_v2(Bucket=bucket, Prefix=prefix)['Contents']:
            try:
                file_name = obj['Key']
                data = s3.get_object(Bucket=bucket, Key=file_name)
                payload.append(json.loads(data['Body'].read()))
            except Exception as e:
                pass
        payload = [item for sublist in payload for item in sublist]
        payload_pqt = toparquet(payload)
        
        save_file_to_s3('maks-alexandria-data-staged', outputkey, payload_pqt)
        return {
            'statusCode': 200,
            'isError': False,
            'body': 'Finished Downloading'
            }
    except Exception as e:
        return {
            'statusCode': 400,
            'isError': True,
            'body': 'Downloading Failed'
        }
