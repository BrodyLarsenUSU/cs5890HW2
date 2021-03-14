import time
import json
import boto3
import sys
import logging

whereStored = sys.argv[1]
arrival = sys.argv[2]
destination = sys.argv[3]
origin = "s3"

keyList = []
logging.basicConfig(filename="request.logs", level=logging.INFO, format='%(asctime)s')

def checkOrigin():
    S3API = boto3.client(origin, region_name="us-east-1")
    if S3API.list_objects_v2(Bucket=arrival)["KeyCount"] == 0:
        return False
    else:
        return True

def putInS3():
    logging.info('started')
    S3API = boto3.client(origin, region_name="us-east-1")
    x = checkOrigin()
    if x == False:
        print("Error: Origin is empty")
    while x == True:
        for keys in S3API.list_objects_v2(Bucket=arrival)['Contents']:
            keyList.append(keys["Key"])

        for obj in keyList:
            try:

                content_object = S3API.get_object(Bucket=arrival, Key=obj)
                file_content = content_object['Body'].read().decode('utf-8')
                json_content = json.loads(file_content)
                jsonString = json.dumps(json_content)
                S3API.delete_object(Bucket=arrival, Key=obj)

                # type = json_content["type"]
                # owner = json_content["owner"]
                objID = json_content["widgetId"]

                widgetID = "widget/Brody/" + objID + ".json"

                print(json_content["type"])

                S3API.put_object(Bucket=destination, Key=widgetID, Body=jsonString)
                x = checkOrigin()
                if x == False:
                    print("Error: Origin is temporarily empty, waiting 3 seconds and trying again")
                    time.sleep(3)
                x = checkOrigin()
                if x == False:
                    print("Error: origin is not filling up with requests")
            except:
                print("no such key")
                logging.info('Finished')



def putInDynamodb():
    try:
        S3API = boto3.client(origin, region_name="us-east-1")
        for keys in S3API.list_objects_v2(Bucket=arrival)['Contents']:
            keyList.append(keys["Key"])
    except:
        print("error: origin is empty: waiting 3 seconds to trying again")
        time.sleep(3)


    for keys in keyList:
        keyList.append(keys["Key"])
    if len(keyList) == 0:
        print("Origin is temporarily empty: waiting 5 seconds")
        time.sleep(5)
    if len(keyList) == 0:
        print("Origin point is still empty: Ending program")
        pass
    else:
        if len(keyList) == 0:
            print("Origin is temporarily empty: waiting 5 seconds")
            time.sleep(5)
        if len(keyList) == 0:
            print("Origin point is still empty: Ending program")
            pass
        for obj in keyList:
            if len(keyList) == 0:
                print("Origin is temporarily empty: waiting 5 seconds")
                time.sleep(5)
            if len(keyList) == 0:
                print("Origin point is still empty: Ending program")
                pass
            content_object = S3API.get_object(Bucket=arrival, Key=obj)
            file_content = content_object['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            # jsonString = json.dumps(json_content)
            S3API.delete_object(Bucket=arrival, Key=obj)

            type = json_content["type"]
            owner = json_content["owner"]
            objID = json_content["widgetId"]

            # widgetID = "widget/Brody/" + objID + ".json"

            print(json_content["type"])

            # request = '{"type": ' + type + ', "widget_id": ' + objID + ', "owner": ' + owner + '}'
            db = boto3.resource("dynamodb", region_name="us-east-1")
            table = db.Table(destination)
            table.put_item(Item=json_content)


if whereStored == "s3":
    putInS3()



elif whereStored == "dynamodb":
    try:
        putInDynamodb()
    except:
        print("Error: Origin point is empty")

logging.debug('Finished')

#TODO Widget Create Request: create, update, or delete

