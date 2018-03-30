#DynamoDB Query Script written and tested using python 3.6
import argparse
import boto3
import boto.s3
from boto.s3.key import Key

S3_FILE_LOCATION_FIELD_DESCRIPTOR = 'Item.'

def S3FileDownload(pathToS3BucketLocation):

    #split path to S3 bucket location to assign paramters
    #assumption is made that there are no subfolder in bucket!!!

    pathList = pathToS3BucketLocation.split("/")
    fileName = pathList[len(pathList) - 1]
    bucketName = pathList[len(pathList) - 2]

    fileKey = fileName.rsplit(".", 1)[0]
    print("Key Name of File Object: " + fileKey)

    s3 = boto3.resource('s3')
    s3.meta.client.download_file(bucketName, fileKey, fileName)
    return

def queryTable(tableName):
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(tableName)
    print(table.creation_date_time)
    return

def retrieveDynamoDBItem(tableName, modelName, timeStamp):
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tableName)
    response = table.get_item(
        Key={
            'modelName': modelName,
            'timeStamp': timeStamp
        }
    )
    item = response['Item']
    pathToS3BucketLocation = item[S3_FILE_LOCATION_FIELD_DESCRIPTOR]
    print(item)
    return pathToS3BucketLocation

def main():

    # command line arguments
    parser = argparse.ArgumentParser(description='Query for records in dynamoDB tables. Locate s3 bucket file location and download locally')
    parser.add_argument('modelName', help='Dynamo db table name')
    parser.add_argument('timeStamp', help='Time Stamp that is to be associated with this CSV File [YYYY-MM]')
    parser.add_argument('tableName', default='testz', type=str, nargs='?', help='Enter table name here')
    parser.add_argument('region', default='us-west-2', nargs='?', help='Dynamo db region name (default=us-west-2')
    args = parser.parse_args()
    print(args)

    modelName = args.modelName
    timeStamp = args.timeStamp
    tableName = args.tableName

    #Query to see if table can be located
    queryTable(tableName)

    #Retreive S3 Bucket File Location from DynamoDB Query
    pathToS3BucketLocation = retrieveDynamoDBItem(tableName, modelName, timeStamp)
    print(pathToS3BucketLocation)

    #Download S3 Bucket Binary File
    S3FileDownload(pathToS3BucketLocation)

    #TODO
    # Verify that user entered correct format for timestamp
    # Else exit program and print error log to console

    ##Verifying that binary file was successfully downloaded from S3 bucket
    #try:
    #    with open(args.csvFile) as file:
    #        print("csv file was found")
    #        csvFile = args.csvFile
    #        binaryOutputFile = csvFile.rsplit(".", 1)[0]
    #        print(binaryOutputFile)
    #        binaryOutputFile = binaryOutputFile + "binary.csv"
    #except IOError as e:
    #    print("The argument " + args.csvFile + " passed to csvIngester_toDynamoDb.py script does not exist or is locked.")

if __name__ == "__main__":
    main()