# Dynamic PartitionedFileSet Parquet Batch Sink


Description
-----------
Sink for a ``PartitionedFileSet`` that writes data in Parquet format
and leverages one or more record field values for creating partitions.
All data for the run will be written to a partition based on the
specified fields and their value.


Use Case
--------
This sink is used whenever you want to write to a ``PartitionedFileSet`` in Parquet format
using a value from the record as a partition. For example, you might want to load historical
data from a database and partition the dataset on the original creation date of the data.


Properties
----------
**name:** Name of the ``PartitionedFileSet`` to which records are written.
If it doesn't exist, it will be created.

**schema:** The Avro schema of the record being written to the sink as a JSON Object.

**basePath:** Base path for the ``PartitionedFileSet``. Defaults to the name of the dataset.

Example
-------
This example will write to a ``PartitionedFileSet`` named ``'users'`` using the original
``'create_date'`` of the record as the partition:

    {
        "name": "DynamicPFSParquet",
        "type": "batchsink",
        "properties": {
            "name": "users",
            "fieldNames": "create_date",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"user\",
                \"fields\":[
                    {\"name\":\"id\",\"type\":\"long\"},
                    {\"name\":\"name\",\"type\":\"string\"},
                    {\"name\":\"create_date\",\"type\":\"string\"}
                ]
            }"
        }
    }

It will write data in Parquet format using the given schema. Every time the pipeline runs, 
partitions will be determined in the ``PartitionedFileSet`` based on the ``create_date``
of each record.