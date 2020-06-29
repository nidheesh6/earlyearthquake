# Quakify

Real time seismic sensor streaming platform to detect and notify about earthquakes .

[Link](https://docs.google.com/presentation/d/1QrUJkKewzaLInuhdTyb9JTEMNlFi4kaIZksZlDQ-o4Y/edit?usp=sharing) to my presentation.

<hr/>

## How to install and get it up and running


<hr/>

## Introduction
The goal of the project is to detect the earthquake in the real-time using the data from the IoT SENSORS and notify the people before the earthquake is felt. The fact which makes this project possible is, the time taken to send, process and distribute the data is less comapared to the time taken by the seismic waves generated by earth quake to reach from one place to other.The notify period can be varied between a few seconds to minutes. 

## Architecture
![Architecture](https://github.com/nidheesh6/earlyearthquake/blob/master/documents/pipeline.png)

The data from the Amazon S3 is streamed using kafka in to the spark.In the spark ,the processing of the sensor data takes place and a value called 'gal' is calculated which is used to predict the earthquake.The gal values greater than 3 are categorized as earthquake and a notifications are sent to the phones or emails using the Amazon Simple Notificaion Service. The data processed in the spark is stored in the postges for the historical analysis.

## Dataset
The grillo community has deployed the IoT devices in three various countries. The data from these devices is stored in the Amazon S3 in jsonl format.The size of the amazon bucket is one Terabyte.The bucket is updated for every 5 minutes.the ARN for the amazon S3 bucket is arn:aws:s3:::grillo-openeew.
 
## Engineering challenges

## Trade-offs
