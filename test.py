import consumer
import sys


#Test if
whereStored = "s3"
arrival = "usu-cs5260-rabbits-requests"
destination = "usu-cs5260-rabbits-web"
origin = "s3"

sys.argv[1] = whereStored
sys.argv[2] = arrival
sys.argv[3] = destination
consumer.Consumer(sys.argv[1], sys.argv[2], sys.argv[3], origin)


