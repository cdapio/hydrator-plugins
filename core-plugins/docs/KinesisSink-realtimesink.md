# Amazon Kinesis Stream Real-time Sink


Description
-----------
Real-time sink that outputs to a specified Amazon Kinesis Stream.

Use Case
--------
This sink is used when you want to write to a kinesis stream in real-time. For example, you
may want to read data from a CDAP stream or any realtime source and write it to a kinesis.


Properties
----------
**name:** The name of the kinesis stream to output to. Must be a valid kinesis stream name. The kinesis stream
will be created if it does not exist.

**bodyField:** Name of the field in the record that contains the data to be written to the specified kinesis stream.
The data could be in binary format as a byte array, a ByteBuffer or it can be any primitive type.

**accessID:** The access Id provided by AWS required to access the kinesis streams.

**accessKey:** AWS access key secret having access to Kinesis streams.

**partitionKey:** Partition key to identify shard.

**shardCount:** Number of shards to be created, each shard has input of 1mb/s.

Example
-------
This example will write to a kinesis stream named 'MyKinesisStream'. The kinesis stream will be created if it does not
exists already. Each record it receives will be written as a single stream event. The stream event body will be equal
to the value of the 'message' field from the input record::

    {
        "name": "MyKinesisStream",
        "type": "KinesisSink",
        "properties": {
            "name": "purchases",
            "bodyField": "message",
            "accessID": "my_aws_access_key",
            "accessKey": "my_aws_access_secret",
            "shardCount": "1",
            "partitionKey": "abc"
        }
    }
