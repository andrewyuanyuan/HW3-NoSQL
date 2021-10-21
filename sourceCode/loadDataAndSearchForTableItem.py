import csv
import boto3

s3 = boto3.resource('s3',
    aws_access_key_id='my_access_key_id',
    aws_secret_access_key='my_secret_access_key'
)

dyndb = boto3.resource('dynamodb',
    region_name='us-east-2',
    aws_access_key_id='my_access_key_id',
    aws_secret_access_key='my_secret_access_key'
)

"""
@brief Read the csv file, uploading the blobs and creating.
"""

with open("experiments.csv", 'r', encoding='utf-8') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        # print(item)
        body = open(r"C:\Users\andre\Documents\GitHub\HW3---NoSQL\datafiles\\"+item[4], 'rb')
        s3.Object('anyuanyu-bucket', item[4]).put(Body=body)
        md = s3.Object('anyuanyu-bucket', item[4]).Acl().put(ACL='public-read')

        url = "https://s3-us-east-2.amazonaws.com/anyuanyu-bucket/"+item[4]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'conductivity' : item[2], 'concentration' : item[3], 'url':url}

        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")


"""
@brief Search for the items in table .
"""

table = dyndb.Table("DataTable")
response = table.get_item(
    Key={
        'PartitionKey': "3",
        'RowKey': "-2.93"
    }
)
print(response['Item'])
