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
with the name user customized in a specified bucket in google cloud storage.


Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**projectId:** Google Cloud Project ID which has the access to a specified bucket. (Macro-enabled)

**jsonKey:** The JSON certificate file of the service account used for GCS access. See Google's documentation
on [Service account credentials](https://cloud.google.com/storage/docs/authentication#generating-a-private-key) for details.
Note that user should upload his credential file to the same path of all nodes. (Macro-enabled)

**bucketKey:** The bucket inside google cloud storage to store the data.

**bucketDir:** the directory inside the bucket where the data is stored. Need to be a new directory. (Macro-enabled)

**fileSystemProperties:** A JSON string representing a map of properties needed for the
distributed file system. The property names needed for Google Cloud Storage (*projectID* and *jsonKeyFile*)
will be included as ``'fs.gs.project.id'`` and ``'google.cloud.auth.service.account.json.keyfile'``. (Macro-enabled)

**schema:** The Avro schema of the record being written to the sink as a JSON object.


Example
-------
This example will write to an Google Cloud Storage output located at gs://bucket/directory.
It will write data in Avro format using the given schema. Every time the pipeline runs, user
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
                {\"name\":\"body\",\"type\":\"string\"}]}",
                "BucketKey": "bucket",
                "BucketDir": "directory",
                "ProjectId": "projectid",
                "JsonKeyFile": "path/to/jsonKeyFile",
                "referenceName": "name"
         }
      }
    }
