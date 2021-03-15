import time
import json
import boto3
import sys
import logging

#These are used when consumer is not in test mode: These should be commented out when trying to run unit tests
# whereStored = sys.argv[1]
# arrival = sys.argv[2]
# destination = sys.argv[3]
# origin = "s3"


def Consumer(whereStored, arrival, destination, origin):

    #setting up log information
    logging.basicConfig(filename="request.logs", format='%(asctime)s:%(message)s', level=logging.DEBUG)
    logging.debug('started')

    keyList = []

    #check origin check to see if the "requests" s3 bucket is empty and returns a True or False
    def checkOrigin():
        S3API = boto3.client(origin, region_name="us-east-1")
        if S3API.list_objects_v2(Bucket=arrival)["KeyCount"] == 0:
            return False
        else:
            return True

    #Pulls requests from s3 bucket and creates widgets to put into an s3 bucket
    def putInS3():
        S3API = boto3.client(origin, region_name="us-east-1")

        #check to see if request bucket is initially empty. Program will terminate if bucket starts empty.
        x = checkOrigin()
        if x == False:
            print("Error: Origin is empty")
        while x == True:

            #creates a list of keys for all the request objects objects that were in the request bucket
            #new keys will be added and checked after every iteration of the while loop
            for keys in S3API.list_objects_v2(Bucket=arrival)['Contents']:
                keyList.append(keys["Key"])

            #use the key from the keylist to pull an object from the s3 bucket
            for obj in keyList:

                #this try block catches and reports errors.
                try:
                    #pulls s3 obj and convert into a json file so information can be read
                    content_object = S3API.get_object(Bucket=arrival, Key=obj)
                    file_content = content_object['Body'].read().decode('utf-8')
                    json_content = json.loads(file_content)
                    jsonString = json.dumps(json_content)
                    S3API.delete_object(Bucket=arrival, Key=obj) #deletes request after information is taken from request

                    #creates new json to push to specified s3 bucket
                    objID = json_content["widgetId"]
                    widgetID = "widget/Brody/" + objID + ".json"
                    print("creating a widget " + json_content["type"] + " request")
                    S3API.put_object(Bucket=destination, Key=widgetID, Body=jsonString)

                    #checks again is request bucket is empty. If empty the program will wait 3 seconds before checking again
                    #If the request bucket has objects it will continue to the next object in line
                    #If empty after 3 seconds program terminates
                    x = checkOrigin()
                    if x == False:
                        print("Error: Origin is temporarily empty, waiting 3 seconds and trying again")
                        time.sleep(3)
                    x = checkOrigin()
                    if x == False:
                        print("Error: origin is not filling up with requests")

                except Exception as error:
                    print(error)
                    time.sleep(.1) #wait 100ms before trying the next s3 object

    # Pulls requests from s3 bucket and creates widgets to put into a dynamodb table
    def putInDynamodb():
        S3API = boto3.client(origin, region_name="us-east-1")

        # check to see if request bucket is initially empty. Program will terminate if bucket starts empty.
        x = checkOrigin()
        if x == False:
            print("Error: Origin is empty")
        while x == True:

            # creates a list of keys for all the request objects objects that were in the request bucket
            # new keys will be added and checked after every iteration of the while loop
            for keys in S3API.list_objects_v2(Bucket=arrival)['Contents']:
                keyList.append(keys["Key"])

            # use the key from the keylist to pull an object from the s3 bucket
            for obj in keyList:

                # this try block catches and reports errors.
                try:
                    # pulls s3 obj and convert into a json file so information can be read
                    content_object = S3API.get_object(Bucket=arrival, Key=obj)
                    file_content = content_object['Body'].read().decode('utf-8')
                    json_content = json.loads(file_content)
                    S3API.delete_object(Bucket=arrival, Key=obj)#deletes request after information is taken from request

                    print("creating a widget " + json_content["type"] + " request")

                    # pushes json content to specified dynamodb table
                    db = boto3.resource("dynamodb", region_name="us-east-1")
                    table = db.Table(destination)
                    table.put_item(Item=json_content)

                    # checks again is request bucket is empty. If empty the program will wait 3 seconds before checking again
                    # If the request bucket has objects it will continue to the next object in line
                    # If empty after 3 seconds program terminates
                    x = checkOrigin()
                    if x == False:
                        print("Error: Origin is temporarily empty, waiting 3 seconds and trying again")
                        time.sleep(3)
                    x = checkOrigin()
                    if x == False:
                        print("Error: origin is not filling up with requests")

                except Exception as error:
                    print(error)
                    time.sleep(.1) #wait 100ms before trying the next s3 object

    #checks user input to determine where to store widgets
    if whereStored == "s3":
        putInS3()

    elif whereStored == "dynamodb":
        putInDynamodb()

    else:
        print("Error: invalid method of storage")
        print("s3 or dynamodb are available")

    logging.debug('finished')


