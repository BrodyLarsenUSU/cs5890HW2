

#TODO: store widgets in bucket
#TODO: store widgets in dynamedb table

#TODO: create user interface that asks: where to store widgets (bucket or database)? if bucket what bucket, list off
# bucket names. Use commandline. user may need to change bucket 1's name?
# input = sys.argv[1]
# if len(input) <= 2:
#     print("too few inputs. required inputs: {bucket or database} {Which bucket to pull from} {destination bucket-name"
#           "or database-name")
# print(sys.argv)
import secrets
import json
import boto3
import sys

whereStored = sys.argv[1]
arrival = sys.argv[2]
destination = sys.argv[3]
origin = "s3"

# print(key)
keyList = []

if whereStored == "s3":
    try:
        S3API = boto3.client(origin, region_name="us-east-1")
        for keys in S3API.list_objects_v2(Bucket=arrival)['Contents']:
            keyList.append(keys["Key"])
        for obj in keyList:
            content_object = S3API.get_object(Bucket=arrival, Key=obj)
            file_content = content_object['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            jsonString = json.dumps(json_content)
            S3API.delete_object(Bucket=arrival, Key=obj)

            type = json_content["type"]
            owner = json_content["owner"]
            objID = json_content["widgetId"]

            widgetID = "widget/Brody/" + objID + ".json"

            print(json_content["type"])

            request = '{"type": ' + type + ', "widget_id": ' + objID + ', "owner": ' + owner +'}'
            object = S3API.put_object(Bucket=destination, Key=widgetID, Body=jsonString)


    finally:
        print('hi')

elif whereStored == "dynamodb":
    S3API = boto3.client(origin, region_name="us-east-1")
    for keys in S3API.list_objects_v2(Bucket=arrival)['Contents']:
        keyList.append(keys["Key"])
    for obj in keyList:
        content_object = S3API.get_object(Bucket=arrival, Key=obj)
        file_content = content_object['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        jsonString = json.dumps(json_content)
        # S3API.delete_object(Bucket=arrival, Key=obj)

        type = json_content["type"]
        owner = json_content["owner"]
        objID = json_content["widgetId"]

        widgetID = "widget/Brody/" + objID + ".json"

        print(json_content["type"])

        request = '{"type": ' + type + ', "widget_id": ' + objID + ', "owner": ' + owner + '}'
        db = boto3.resource("dynamodb", region_name="us-east-1")
        table= db.Table(destination)
        table.put_item(Item = json_content)




#TODO read objects (a widget request) from bucket 1 key order. Requests are in json text. use json parser.
#TODO Widget Create Request: create, update, or delete

#widget needs to contain all data found in Widget Create Request
#When awidget needs tobe stored in Bucket 2, you may serialize it into any format that you’d like, e.g., JSON, XML, binary, etc.
# Its key should be based on the following pattern: widgets/{owner}/{widget id}