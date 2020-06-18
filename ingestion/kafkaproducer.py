" stream data from Amazon S3 to kafka"
import json
import kafka
import boto3

class KafkaProducer:
    """
        sends data from S3 to kafka 
    """
    def __init__(self):
        """
            initialize bootstrap_servers,topicname for kafka
        """
        self.bootsrap_servers = ['localhost:9092']
        self.topic_name = 'pressure_sensor'
        self.producer = kafka.KafkaProducer(bootstrap_servers=self.bootstrap_servers)

    def produce_stream(self):
        """
            read data from s3 bucket GRILLO-OPENEEW and send data to kafka.
            boto3 library used for reading the objects from S3.
        """
        s3_service = boto3.resource('s3')
        bucket_name = s3_service.Bucket('grillo-openeew')
        for obj in bucket_name.objects.all():
            key = obj.key
            if key not in ['devices/country_code=cl/devices.jsonl', \
                    'devices/country_code=cr/devices.jsonl', \
                     'devices/country_code=mx/devices.jsonl', 'index.html']:
                body = obj.get()['Body'].read().decode("utf-8")
                for line in body.splitlines():
                    jsondata = json.loads(line)
                    data = json.dumps(jsondata)
                    self.producer.send(self.topic_name, value=data)
if __name__ == "__main__":
    PROD = KafkaProducer()
    PROD.produce_stream()
