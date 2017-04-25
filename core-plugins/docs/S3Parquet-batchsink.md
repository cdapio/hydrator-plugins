# Amazon S3 Parquet Batch Sink


Description
-----------
A batch sink to write to S3 in Parquet format.


Use Case
--------
This source is used whenever you need to write to Amazon S3 in Parquet format. For example,
you might want to create daily snapshots of a database by reading the entire contents of a
table, writing to this sink, and then other programs can analyze the contents of the
specified file. The output of the run will be stored in a directory with suffix
'yyyy-MM-dd-HH-mm' from the base path provided.


Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**authenticationMethod:** Authentication method to access S3. Defaults to Access Credentials.
 User need to have AWS environment only to use IAM role based authentication.
 For IAM, URI scheme should be s3a://. (Macro-enabled)

**accessID:** Access ID of the Amazon S3 instance to connect to. (Macro-enabled)

**accessKey:** Access Key of the Amazon S3 instance to connect to. (Macro-enabled)

**basePath:** The S3 path where the data is stored. Example: 's3n://logs'. (Macro-enabled)

**enableEncryption:** Server side encryption. Defaults to True. Sole supported algorithm is AES256. (Macro-enabled)

**fileSystemProperties:** A JSON string representing a map of properties needed for the
distributed file system. The property names needed for S3 (*accessID* and *accessKey*)
will be included as ``'fs.s3n.awsSecretAccessKey'`` and ``'fs.s3n.awsAccessKeyId'``. (Macro-enabled)

**pathFormat:** The format for the path that will be suffixed to the basePath; for
example: the format ``'yyyy-MM-dd-HH-mm'`` will create a file path ending in
``'2015-01-01-20-42'``. Default format used is ``'yyyy-MM-dd-HH-mm'``. (Macro-enabled)

**schema:** The Parquet schema of the record being written to the sink as a JSON object. (Macro-enabled)

**compressionCodec:** Optional parameter to determine the compression codec to use on the resulting data. 
Valid values are None, Snappy, GZip, and LZO.


Example
-------
This example will write to an S3 output located at ``s3n://logs``. It will write data in
Parquet format using the given schema. Every time the pipeline runs, a new output directory
from the base path (``s3n://logs``) will be created which will have the directory name
corresponding to the start time in ``yyyy-MM-dd-HH-mm`` format:

    {
        "name": "S3Parquet",
        "type": "batchsink",
        "properties": {
            "accessKey": "key",
            "accessID": "ID",
            "basePath": "s3n://logs",
            "pathFormat": "yyyy-MM-dd-HH-mm",
            "compressionCodec": "Snappy",
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