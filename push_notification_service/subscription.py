""" subscribing the numbers to a topic called earthquake on amazon sns"""
import boto3
"""reading credentials from the file"""
f = open("credentials.txt", "r")
lines = f.readlines()
AWS_REGION = lines[0]
ACCESS_KEY = lines[1]
SECRET_KEY = lines[2]
f.close()
"""reading phone numbers from a file"""
phone_nums_file = open("numbers.text", "r")
list_of_phone_numbers = phone_nums_file.readlines()
client = boto3.client('sns', region_name=AWS_REGION,\
                    aws_access_key_id=ACCESS_KEY, \
                    aws_secret_access_key=SECRET_KEY)
"""subscribing phone numbers to the AWS sns topic"""
for number in list_of_phone_numbers:
    client.subscribe(
        TopicArn="arn:aws:sns:us-east-1:381557116326:earthquake",
        Protocol='sms',
        Endpoint=number  # <-- phone numbers who'll receive SMS.
    )
phone_nums_file.close()


