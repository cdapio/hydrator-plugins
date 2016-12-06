# Amazon Kinesis Stream Real-time Sink


Description
-----------
Kinesis sink that outputs to a specified Amazon Kinesis Stream.

Use Case
--------
This sink is used when you want to write to a kinesis stream.

Properties
----------
**name:** The name of the kinesis stream to output to. Must be a valid kinesis stream name. The kinesis stream will be
created if it does not exist.

**accessID:** The access Id provided by AWS required to access the kinesis streams.

**accessKey:** AWS access key secret having access to Kinesis streams.

**distribute:** Boolean to decide if the data has to be sent to a single shard or has to be uniformly distributed among
all the shards. Default value is true.

**shardCount:** Number of shards to be created, each shard has input of 1mb/s. Default value is 1.

Example
-------
This example will write to a kinesis stream named 'MyKinesisStream'. The kinesis stream will be created if it does not
exists already. Each record it receives will be written as a single stream event. Two shards for this stream will be
created and records will be distributed uniformly across the records::

    {
        "name": "MyKinesisStream",
        "type": "KinesisSink",
        "properties": {
            "name": "purchases",
            "accessID": "my_aws_access_key",
            "accessKey": "my_aws_access_secret",
            "shardCount": "2",
            "distribute": "true"
        }
    }
