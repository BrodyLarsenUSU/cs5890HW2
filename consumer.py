#TODO: store widgets in bucket
#TODO: store widgets in dynamedb table

#TODO: create user interface that asks: where to store widgets (bucket or database)? if bucket what bucket, list off
# bucket names. Use commandline. user may need to change bucket 1's name?

#TODO read objects (a widget request) from bucket 1 key order. Requests are in json text. use json parser.
#TODO Widget Create Request: create, update, or delete

#widget needs to contain all data found in Widget Create Request
#When awidget needs tobe stored in Bucket 2, you may serialize it into any format that you’d like, e.g., JSON, XML, binary, etc.
# Its key should be based on the following pattern: widgets/{owner}/{widget id}