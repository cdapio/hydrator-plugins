# Google BigQuery Batch Source


Description
-----------
Batch source to use Google Cloud Platforms's [BigQuery](https://cloud.google.com/bigquery/docs/) as a Source.

Use Case
--------
This source is used whenever you need to read from a Google BigQuery table or a BigQuery result.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**projectID:** The ID of the project in Google Cloud. (Macro-enabled)

**jsonKeyFilePath:** The credential JSON key file path. (Macro-enabled)
See Google's documentation on [Service account credentials](https://cloud.google.com/storage/docs/authentication#generating-a-private-key) for details.
Note that user should upload his credential file to the same path of all nodes.

**inputTableId:** The BigQuery table to read from, in the form '<projectId (optional)>:<datasetId>.<tableId>'.
The 'projectId' is optional. Example: 'myproject:mydataset.mytable'. Note: if the import query is provided,
then the inputTable should be a blank table with the query result schema so as to store the intermediate result.
If the import query is not provided, BigQuery source will read the content from the inputTable.


this table should already exist and be an empty table with the query result schema.(Macro-enabled)

**tempBucketPath:** The temporary Google Storage directory to be used for storing the intermediate results.
e.g. 'gs://bucketname/directoryname'. The directory should not already exist. Users should manually delete
this directory afterwards to avoid any extra Google Storage charges.(Macro-enabled)

**importQuery:** The SELECT query to use to import data from the specified table.
For example: 'SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words FROM publicdata:samples.shakespeare',
where 'publicdata' is the project name (optional), 'samples' is the dataset name, and 'shakespare' is the table name.
If this query is not provided, reads instead from the configured inputTable.(Macro-enabled)

**outputSchema:** Comma-separated mapping of output schema column names to data types; for example: 'A:string,B:int'.

Example
-------
This example will process the import query on the [publicdata:samples.shakespeare], the query result is then stored in
Google BigQuery inputTable: projectName:datasetName.tableName, then inputTable is downloaded to the tempBucketPath
in Google Cloud Storage and finally CDAP BigQuery Source reads the file in the temporary Google Cloud Storage directory.
User should first create a empty table in Google BigQuery as the inputTable, and the table should have the schema:
title:string, unique_words:int. Make sure to delete the temporary Google Cloud Storage directory after reading.

      {
        "name": "BigQuery",
        "plugin": {
          "name": "BigQuery",
          "type": "batchsource",
          "label": "BigQuery",
          "artifact": {
            "name": "googlecloud-plugins",
            "version": "1.4.0",
            "scope": "SYSTEM"
          },
          "properties": {
            "referenceName": "bigquery",
            "projectId": "projectId",
            "tempBucketPath": "gs://bucketName.datasetName/tableName",
            "jsonFilePath": "/path/to/jsonkeyfile",
            "importQuery":"SELECT TOP(corpus, 10) as title, COUNT(*) as
             unique_words FROM [publicdata:samples.shakespeare]",
            "inputTableId": "projectName:datasetName.tableName",
            "outputSchema": "title:string,unique_words:int"
          }
        }
      }