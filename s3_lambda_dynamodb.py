import json
import boto3
import datetime

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table_name = 'Images'
table_create_params = {
  'TableName': table_name,
  'KeySchema': [
      {'AttributeName': 'image_name', 'KeyType': 'HASH'},
      {'AttributeName': 'image_size', 'KeyType': 'RANGE'},
  ],
  'AttributeDefinitions': [
      {'AttributeName': 'image_name', 'AttributeType': 'S'},
      {'AttributeName': 'image_size', 'AttributeType': 'N'},
  ],
  'ProvisionedThroughput': {
      'ReadCapacityUnits': 2,
      'WriteCapacityUnits': 2
  }
}

def lambda_handler(event, context):
  try:
    bucket = event['Records'][0]['s3']['bucket']['name']
    bucket_obj = event['Records'][0]['s3']['object']
    tables = [table for table in dynamodb.meta.client.list_tables()['TableNames']]
    image_uploaded_ts = datetime.datetime.now()
    if tables.count(table_name) >= 1:
      print('exists')
      image_info = {
        "image_name": bucket_obj['key'],
        "image_size": bucket_obj['size'],
        "image_path": f"s3://{bucket}/{bucket_obj['key']}",
        "image_uploaded_ts": str(image_uploaded_ts)
      }
      dynamodb.meta.client.put_item(TableName=table_name, Item=image_info)
    else:
      table = dynamodb.create_table(**table_create_params)
      print('created')
      table.wait_until_exists()
    return {
      'statusCode': 200,
      'body': json.dumps('Image info successfully added')
    }
  except Exception as err:
    print(err)
    return err