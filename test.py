import subprocess
import consumer
import time

#TEST 1
#Inputs for storing widgets in an s3 bucket
whereStored = "s3"
arrival = "usu-cs5260-rabbits-requests"
destination = "usu-cs5260-rabbits-web"
origin = "s3"

print("Test 1: Checking if pulling requests and making widgets works. Pushing widgets to another s3 bucket")
print("sys.argv[1]: " + whereStored)
print("sys.argv[2]: " + arrival)
print("sys.argv[3]: " + destination)
print("Request origin bucket: " + origin)

#run producerHW2.jar file to repopulate bucket 1 with requests
print("running producerHW2.jar to repopulate bucket 1")
subprocess.call(['java', '-jar', 'producerHW2.jar', '--request-bucket=usu-cs5260-rabbits-requests'])
print('Test 1: Running consumer')
consumer.Consumer(whereStored, arrival, destination, origin)

#TEST 2
#Inputs for storing widgets in a dynamodb table
whereStored = "dynamodb"
arrival = "usu-cs5260-rabbits-requests"
destination = "widgets"
origin = "s3"

print("Test 2: Checking if pulling requests and making widgets works. Pushing widgets to specified Dynamodb table")
print("sys.argv[1]: " + whereStored)
print("sys.argv[2]: " + arrival)
print("sys.argv[3]: " + destination)
print("Request origin bucket: " + origin)

#run producerHW2.jar file to repopulate bucket 1 with requests
print("running producerHW2.jar to repopulate bucket 1")
subprocess.call(['java', '-jar', 'producerHW2.jar', '--request-bucket=usu-cs5260-rabbits-requests'])
print('Test 2: Running consumer')
consumer.Consumer(whereStored, arrival, destination, origin)

#TEST 3
#Inputs for storing widgets in s3 but bucket 1 is empty
whereStored = "s3"
arrival = "usu-cs5260-rabbits-requests"
destination = "usu-cs5260-rabbits-web"
origin = "s3"

print("Test 3: testing how program handles an initially empty request bucket with destination being an s3 bucket")
print("sys.argv[1]: " + whereStored)
print("sys.argv[2]: " + arrival)
print("sys.argv[3]: " + destination)
print("Request origin bucket: " + origin)

print('Test 3: Running consumer')
consumer.Consumer(whereStored, arrival, destination, origin)

#TEST 4
#Inputs for storing widgets in dynamodb table but bucket 1 is empty
whereStored = "s3"
arrival = "usu-cs5260-rabbits-requests"
destination = "usu-cs5260-rabbits-web"
origin = "s3"

print("Test 4: testing how program handles an initially empty request bucket with destination being a dynamodb table")
print("sys.argv[1]: " + whereStored)
print("sys.argv[2]: " + arrival)
print("sys.argv[3]: " + destination)
print("Request origin bucket: " + origin)

print('Test 4: Running consumer')
consumer.Consumer(whereStored, arrival, destination, origin)
