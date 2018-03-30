#python script to ingest csv file into dynamoDb table
#This script was tested with Python 3.6.3
import argparse
import boto3
import boto.s3
import sys
from boto.s3.key import Key
import csv
import time
import pandas

#How to setup credentials file on your computer
#If you have AWS CLI installed this is as simple as calling -> aws config
#You will need to have your keys ready for configuration.
#Alternatively you can have the keys hardcoded into code (not recommended as best practice, depends on application)
#https://boto3.readthedocs.io/en/latest/guide/quickstart.html

###Reference for S3 Bucket Connection###
###https://stackoverflow.com/questions/15085864/how-to-upload-a-file-to-directory-in-s3-bucket-using-boto###

##The way the script is currently configure you are required to manually create the s3 bucket that you are intending for the data file
##to be stored in. Once the s3 bucket is created you can either change the default string in the arg parse piece of code or pass the 
##bucketName of choice as an argument to the script.

#Example Call to Script
#python .\csvIngester_toDynamoDb.py fileToBeUploaded modelName YYYY-MM bucketName  tableName

PATH_TO_AMAZON_S3 = "https://s3.amazonaws.com/"

def appendToDynamoDBTable(tableName, modelName, timeStamp, pathToS3File):
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tableName)
    table.put_item(
        Item={
            'modelName' : modelName,
            'timeStamp' : timeStamp,
            'Item.' : pathToS3File,
        }
    )
    return

def createDynamoDBTable(tableName, PartitionKey, SortKey):
    
    #Get the service resource
    dynamodb_client = boto3.client('dynamodb')

    #Create the DynamoDB table
    try:
        table = dynamodb_client.create_table(
            TableName = tableName,
            KeySchema=[
                {
                    'AttributeName': 'modelName',
                    'KeyType': 'HASH' #Partition key
                },
                {
                    'AttributeName': 'timeStamp',
                    'KeyType' : 'RANGE' #Sort key
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName' : 'modelName',
                    'AttributeType' : 'S'
                },
                {
                    'AttributeName' : 'timeStamp',
                    'AttributeType' : 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits' : 5,
                'WriteCapacityUnits' : 5
            }
        )
    except dynamodb_client.exceptions.ResourceInUseException:
        # table already exist
        print("table already exists in dynamodb")
        return

    #Wait until the table exists.
    #Verify and wait until table is created.sys
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tableName)
    table.meta.client.get_waiter('table_exists').wait(TableName=tableName)
    print("Table creation complete")

    #Print out some data about the table
    print(table.item_count)
    return

def uploadFileToS3Bucket(bucketName, fileName):
    
    #bucketName = AWS_ACCESS_KEY_ID.lower() + "-dump" #bucketName
    keyName = fileName.rsplit(".", 1)[0]
    print("Key Name of File Object: " + keyName)

    # Get the service client
    s3 = boto3.client('s3')

    #s3.create_bucket(Bucket=bucketName)

    # Upload tmp.txt to bucket-name at key-name
    s3.upload_file(fileName, bucketName, keyName)

    pathToS3File = PATH_TO_AMAZON_S3 + bucketName + "/" + fileName
    print ("path to s3 file: " + pathToS3File)
    return pathToS3File

def main():

    # command line arguments
    parser = argparse.ArgumentParser(description='Write CSV records to dynamo db table. CSV Header must map to dynamo table field names.')
    parser.add_argument('binaryFile', help='Name of binary file to upload to S3 bucket')
    parser.add_argument('modelName', help='Dynamo db table name')
    parser.add_argument('timeStamp', help='Time Stamp that is to be associated with this CSV File [YYYY-MM]')
    parser.add_argument('bucketName', default='davari', type=str, nargs='?', help='Enter bucket name here')
    parser.add_argument('tableName', default='testz', type=str, nargs='?', help='Enter table name here')
    parser.add_argument('writeRate', default=5, type=int, nargs='?', help='Number of records to write in table per second (default:5)')
    parser.add_argument('delimiter', default='|', nargs='?', help='Delimiter for csv records (default=|)')
    parser.add_argument('region', default='us-west-2', nargs='?', help='Dynamo db region name (default=us-west-2')
    args = parser.parse_args()
    print(args)

    modelName = args.modelName
    timeStamp = args.timeStamp
    bucketName = args.bucketName
    tableName = args.tableName

    #TODO
    # Verify that user entered correct format for timestamp
    # Else exit program and print error log to console

    ##Verifying that csv file entered by user exist
    try:
        with open(args.binaryFile) as file:
            print("binary file was found")
            binaryFile = args.binaryFile
            #binaryOutputFile = csvFile.rsplit(".", 1)[0]
            #print(binaryOutputFile)
            #binaryOutputFile = binaryOutputFile + "binary.csv"
    except IOError as e:
        print("The argument " + args.binaryFile + " passed to csvIngester_toDynamoDb.py script does not exist or is locked.")

    #convertCSVFileToBinary(csvFile, binaryOutputFile)

    #Call Function To Upload Newly Converted Binary File To S3 Bucket
    pathToS3File = uploadFileToS3Bucket(bucketName, binaryFile)

    ##Only call if create new table for db flag is passed as argument
    #TASK 4 Create Table/Check if Table Already Exist
    createDynamoDBTable(tableName, modelName, timeStamp)

    #TASK 5 Add Entry to Table passing arguments (tableName, modelName, timeStamp, pathToS3File)
    appendToDynamoDBTable(tableName, modelName, timeStamp, pathToS3File)

if __name__ == "__main__":
    main()