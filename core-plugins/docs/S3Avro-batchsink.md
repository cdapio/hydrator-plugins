# Amazon S3 Avro Batch Sink


Description
-----------
A batch sink for writing to Amazon S3 in Avro format.


Use Case
--------
This source is used whenever you need to write to Amazon S3 in Avro format. For example,
you might want to create daily snapshots of a database by reading the entire contents of a
table, writing to this sink, and then other programs can analyze the contents of the
specified file. The output of the run will be stored in a directory with suffix
'yyyy-MM-dd-HH-mm' from the base path provided.


Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**accessID:** Access ID of the Amazon S3 instance to connect to.

**accessKey:** Access Key of the Amazon S3 instance to connect to.

**basePath:** The S3 path where the data is stored. Example: 's3n://logs'.

**fileSystemProperties:** A JSON string representing a map of properties needed for the
distributed file system. The property names needed for S3 (*accessID* and *accessKey*)
will be included as ``'fs.s3n.awsSecretAccessKey'`` and ``'fs.s3n.awsAccessKeyId'``.

**pathFormat:** The format for the path that will be suffixed to the basePath; for
example: the format ``'yyyy-MM-dd-HH-mm'`` will create a file path ending in
``'2015-01-01-20-42'``. Default format used is ``'yyyy-MM-dd-HH-mm'``.

**schema:** The Avro schema of the record being written to the sink as a JSON object.


Example
-------
This example will write to an S3 output located at ``s3n://logs``. It will write data in
Avro format using the given schema. Every time the pipeline runs, a new output directory
from the base path (``s3n://logs``) will be created which will have the directory name
corresponding to the start time in ``yyyy-MM-dd-HH-mm`` format:

    {
        "name": "S3Avro",
        "type": "batchsink",
        "properties": {
            "accessKey": "key",
            "accessID": "ID",
            "basePath": "s3n://logs",
            "pathFormat": "yyyy-MM-dd-HH-mm",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"user\",
                \"fields\":[
                    {\"name\":\"id\",\"type\":\"long\"},
                    {\"name\":\"name\",\"type\":\"string\"},
                    {\"name\":\"birthyear\",\"type\":\"int\"}
                ]
            }"
        }
    }
