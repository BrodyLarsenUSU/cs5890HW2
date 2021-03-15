import time
import json
import boto3
import sys
import logging

whereStored = "s3"
arrival = "usu-cs5260-rabbits-requests"
destination = "usu-cs5260-rabbits-web"
origin = "s3"

#These are used when consumer is not in test mode: These should be commented out when trying to run unit tests
# whereStored = sys.argv[1]
    # arrival = sys.argv[2]
    # destination = sys.argv[3]
    # origin = "s3"

#These are used for unit tests. Comment out when done testing
sys.argv.append(whereStored)
sys.argv.append(arrival)
sys.argv.append(destination)
sys.argv.append(origin)


def Consumer(whereStored, arrival, destination, origin):

    logging.basicConfig(filename="request.logs", format='%(asctime)s:%(message)s', level=logging.DEBUG)
    logging.debug('started')

    keyList = []

    def checkOrigin():
        S3API = boto3.client(origin, region_name="us-east-1")
        if S3API.list_objects_v2(Bucket=arrival)["KeyCount"] == 0:
            return False
        else:
            return True

    def putInS3():
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

                    objID = json_content["widgetId"]

                    widgetID = "widget/Brody/" + objID + ".json"

                    print("creating a widget " + json_content["type"] + " request")

                    S3API.put_object(Bucket=destination, Key=widgetID, Body=jsonString)
                    x = checkOrigin()
                    if x == False:
                        print("Error: Origin is temporarily empty, waiting 3 seconds and trying again")
                        time.sleep(3)
                    x = checkOrigin()
                    if x == False:
                        print("Error: origin is not filling up with requests")

                except Exception as error:
                    print(error)
                    time.sleep(.1)



    def putInDynamodb():
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
                    # jsonString = json.dumps(json_content)
                    S3API.delete_object(Bucket=arrival, Key=obj)

                    # type = json_content["type"]
                    # owner = json_content["owner"]
                    # objID = json_content["widgetId"]

                    print("creating a widget " + json_content["type"] + " request")

                    db = boto3.resource("dynamodb", region_name="us-east-1")
                    table = db.Table(destination)
                    table.put_item(Item=json_content)
                    x = checkOrigin()
                    if x == False:
                        print("Error: Origin is temporarily empty, waiting 3 seconds and trying again")
                        time.sleep(3)
                    x = checkOrigin()
                    if x == False:
                        print("Error: origin is not filling up with requests")
                except Exception as error:
                    print(error)
                    time.sleep(.1)


    if whereStored == "s3":
        putInS3()

    elif whereStored == "dynamodb":
        putInDynamodb()

    else:
        print("Error: invalid method of storage")
        print("s3 or dynamodb are available")


    logging.debug('finished')

#TODO Widget Create Request: create, update, or delete

