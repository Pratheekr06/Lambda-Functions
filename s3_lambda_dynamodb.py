import json
import boto3

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
params = {
  **table_create_params,
  'KeySchema': [
      *table_create_params['KeySchema'],
      {'AttributeName': 'image_path', 'KeyType': 'HASH'}
    ],
  'AttributeDefinitions': [
     *table_create_params['AttributeDefinitions'],
      {'AttributeName': 'image_path', 'AttributeType': 'S'}
    ],
}
def lambda_handler(event, context):
  bucket = event['Records'][0]['s3']['bucket']['name']
  bucket_obj = event['Records'][0]['s3']['object']
  tables = [table for table in dynamodb.meta.client.list_tables()['TableNames']]
  try:
    if tables.count(table_name) >= 1:
      print('exists')
      image_info = {
        "image_name": bucket_obj['key'],
        "image_size": bucket_obj['size'],
        "image_path": f"s3://{bucket}/{bucket_obj['key']}"
      }
      dynamodb.meta.client.put_item(TableName=table_name, Item=image_info)
    else:
      image_data = {
        "image_name": bucket_obj['key'],
        "image_size": bucket_obj['size'],
      }
      table = dynamodb.create_table(**table_create_params)
      print('created')
      table.wait_until_exists()
      print('waited')
      table.put_item(Item=image_data)
    return {
      'statusCode': 200,
      'body': json.dumps('Image info successfully added')
    }
  except Exception as err:
    return {
      'statusCode': 400,
      'body': json.dumps(err)
    }
