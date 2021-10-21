import boto3

"""
@brief Create an s3 instance object
"""

s3 = boto3.resource('s3',
    aws_access_key_id='my_access_key_id',
    aws_secret_access_key='my_secret_access_key'
)

"""
@brief Create a bucket here
"""

try:
    s3.create_bucket(Bucket='anyuanyu-bucket', CreateBucketConfiguration={'LocationConstraint': 'us-east-2'})
except Exception as e:
    print (e)

"""
@brief Try to upload a test file to our bucket
"""

bucket = s3.Bucket("anyuanyu-bucket")
bucket.Acl().put(ACL='public-read')

body = open(r"C:\Users\andre\Desktop\test.txt", 'rb')
o = s3.Object('anyuanyu-bucket', 'test').put(Body=body)
s3.Object('anyuanyu-bucket', 'test').Acl().put(ACL='public-read')

"""
@brief Create the DynamoDB table

Creating the resource does not create the able, only try-block creates the table.
We need to provide a key schema. One element is hashed to produce a partition that stores
a row while the second key is RowKey. The pair (PartitionKey, RowKey) is a unique identifier
for the row in the table.
"""

dyndb = boto3.resource('dynamodb',
    region_name='us-east-2',
    aws_access_key_id='AKIA4KYR4JH4K5K5VTNR',
    aws_secret_access_key='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'
)

try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }       
    )
except Exception as e:
    print (e)
    #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("DataTable")

#wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print(table.item_count)
