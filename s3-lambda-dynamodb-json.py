import boto3
import json
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table_name = 'Employees'
params = {
        'TableName': table_name,
        'KeySchema': [
            {'AttributeName': 'emp_id', 'KeyType': 'HASH'},
            {'AttributeName': 'age', 'KeyType': 'RANGE'}
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'emp_id', 'AttributeType': 'S'},
            {'AttributeName': 'age', 'AttributeType': 'N'}
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 2,
            'WriteCapacityUnits': 2
        }
    }
def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        json_file_name = event['Records'][0]['s3']['object']['key']
        json_object = s3_client.get_object(Bucket=bucket,Key=json_file_name)
        jsonFileReader = json_object['Body'].read()
        jsonData = json.loads(jsonFileReader)
        tables = [table for table in dynamodb.meta.client.list_tables()['TableNames']]
        if tables.count(table_name) >= 1:
            employee_info = jsonData['employee_data']
            table = dynamodb.Table(table_name)
            with table.batch_writer() as writer:
                for e in employee_info:
                    writer.put_item(Item=e)
        else:
            table = dynamodb.create_table(**params)
            table.wait_until_exists()
        return {
        'statusCode': 200,
        'body': json.dumps('Employee info successfully added')
        }
    except Exception as err:
        print(err)
        return err