# Google Cloud Storage Avro Batch Sink


Description
-----------
A batch sink for writing to Google Cloud Storage in Avro format.


Use Case
--------
This source is used whenever you need to write to Google Cloud Storage in Avro format.
For example, you might want to create daily snapshots of a database by reading the
entire contents of a table, writing to this sink, and then other programs can analyze
the contents of the specified file. The output of the run will be stored in a directory
in a specified bucket in Google Cloud Storage.


Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**projectId:** Google Cloud Project ID which has access to the specified bucket. (Macro-enabled)

**jsonKeyFilePath:** The JSON certificate file of the service account used for GCS access.
See Google's documentation on 
[Service account credentials](https://cloud.google.com/storage/docs/authentication#generating-a-private-key)
for details. Note: you should upload the credential file to the same path on all nodes. (Macro-enabled)

**bucketKey:** The bucket inside Google Cloud Storage used to store the data.

**bucketDir:** The directory inside the bucket where the data is to be stored. Needs to be a new directory. (Macro-enabled)

**fileSystemProperties:** A JSON string representing a map of properties needed for the
distributed file system. The property names needed for Google Cloud Storage (*projectId* and *jsonKeyFilePath*)
will be included as ``'fs.gs.project.id'`` and ``'google.cloud.auth.service.account.json.keyfile'``. (Macro-enabled)

**schema:** The Avro schema of the record being written to the sink, as a JSON object.


Example
-------
This example will write to an Google Cloud Storage output located at ``gs://bucket/directory``.
It will write data in Avro format using the given schema. Every time the pipeline runs, a user
should specified a new directory name.

    {
        "name": "GCSAvro",
        "plugin": {
            "name": "GCSAvro",
            "type": "batchsink",
            "label": "GCSAvro",
            "artifact": {
                "name": "core-plugins",
                "version": "1.4.0-SNAPSHOT",
                "scope": "SYSTEM"
            },
            "properties": {
                "schema": "{
                    \"type\":\"record\",
                    \"name\":\"etlSchemaBody\",
                    \"fields\":[
                        {\"name\":\"ts\",\"type\":\"long\"},
                        {\"name\":\"body\",\"type\":\"string\"}
                    ]
                }",
                "bucketKey": "bucket",
                "bucketDir": "directory",
                "projectId": "projectid",
                "jsonKeyFilePath": "path/to/jsonKeyFile",
                "referenceName": "name"
            }
        }
    }
