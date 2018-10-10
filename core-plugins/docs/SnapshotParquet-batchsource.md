# Snapshot Parquet Batch Source


Description
-----------
A batch source that reads from a corresponding SnapshotParquet sink.
The source will only read the most recent snapshot written to the sink.


Use Case
--------
This source is used whenever you want to read data written to the corresponding
SnapshotParquet sink. It will read only the last snapshot written to that sink. For
example, you might want to create daily snapshots of a database by reading the entire
contents of a table and writing it to a SnapshotParquet sink. You might then want to use
this source to read the most recent snapshot and run a data analysis on it.


Properties
----------
**Dataset Name:** Name of the PartitionedFileSet to read from.
If it doesn't exist, it will be created. (Macro-enabled)

**Snapshot Base Path:** Base path for the PartitionedFileSet. Defaults to the name of the dataset. (Macro-enabled)

**FileSet Properties:** Advanced feature to specify any additional properties that should be used with the source,
specified as a JSON object of string to string. These properties are set on the dataset if one is created.
The properties are also passed to the dataset at runtime as arguments. (Macro-enabled)

**Schema:** The Parquet schema of the record being written to the sink as a JSON object.


Example
-------
This example will read from a SnapshotFileSet named 'users'. It will read data in Parquet format
using the given schema. Every time the pipeline runs, only the most recently added snapshot will
be read:

    {
        "name": "SnapshotParquet",
        "type": "batchsource",
        "properties": {
            "name": "users",
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
