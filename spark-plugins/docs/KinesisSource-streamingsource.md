# Amazon kinesis spark streaming source

Description
-----------
Spark streaming source that reads from AWS kinesis streams.

Use Case
--------
This source is used when you want to read data from a kinesis stream in real-time. For example, you
may want to read data from a kinesis stream write it to a cdap dataset.

Properties
----------
**appName:** The name of the kinesis application. The application name that is used to checkpoint the Kinesis sequence
numbers in DynamoDB table

**streamName:** The name of the Kinesis stream to the get the data from. The stream should be active.

**duration:** The interval in milliseconds at which the Kinesis Client Library saves its position in the stream.

**endpointUrl:** Valid Kinesis endpoints URL eg. kinesis.us-east-1.amazonaws.com

**accessID:** The access Id provided by AWS required to access the kinesis streams. The Id can be stored in Cdap secure
store and can be provided as macro configuration.

**accessKey:** AWS access key secret having access to Kinesis streams. The key can be stored in Cdap secure store and
can be provided as macro configuration.

**initialPosition:** Initial position in the stream. Can be either TRIM_HORIZON or LATEST, Default position will be
Latest

Example
-------
This example will read from kinesis stream named 'MyKinesisStream'. It will spin up 1 Kinesis Receiver per shard for the
given stream. It starts pulling from the last checkpointed sequence number of the given stream.

    {
        "name": "MyKinesisStream",
        "type": "KinesisSource",
        "properties": {
            "appName": "myKinesisApp",
            "streamName": "MyKinesisStream",
            "accessID": "my_aws_access_key",
            "accessKey": "my_aws_access_secret",
            "endpointUrl": "1",
            "duration": "2000",
            "initialPosition": "Latest"
        }
    }
