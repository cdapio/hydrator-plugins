# Google BigQuery Batch Source


Description
-----------
Batch source to use Google BigQuery as a Source.
[link to BigQuery Docs](https://cloud.google.com/bigquery/docs/)

Use Case
--------
This source is used whenever you need to read from a Google BigQuery table or a BigQuery result.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**projectID:** The ID of the project in Google Cloud. (Macro-enabled)

**jsonKeyFilePath:** The credential json key file path. (Macro-enabled)
[How to get the keyfile](https://cloud.google.com/storage/docs/authentication#generating-a-private-key)

**inputTableId:** The BigQuery table to read from, in the form [optional projectId]:[datasetId].[tableId], Example:
publicdata:samples.shakespeare. Note that if the import query is specified, this table should be a empty table
with the query result schema. User need to first create such a table.(Macro-enabled)

**tempBucketPath:** The temporary Google Cloud Storage (GCS) directory to store the intermediate result.
e.g. gs://bucketname/directoryname. The directory should not exist.
shUser should delete this directory afterward manually to avoid extra google storage charge. (Macro-enabled)

**importQuery:** The SELECT query to use to import data from the specified table. e.g.
SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words FROM [publicdata:samples.shakespeare].
'publicdata' is the project name, smalples is the dataset name, shakespare is the table name.
This is optional, if empty, just read the  inputTable configured. (Macro-enabled)

**outputSchema:** Comma separated mapping of column names in the output schema to the data types; for example:
'A:string,B:int'. (Macro-enabled)

Example
-------
This example will process the import query on the [publicdata:samples.shakespeare], store the result in the inputTable,
then download the inputTable to the tempBucketPath in Google Cloud Storage and finally reads the file in the temporary
Google Cloud Storage directory. User should first create a empty table in BigQuery as the inputTable, and the table
should have the schema: title:string, unique_words:int. Make sure to delete the temporary Google Cloud Storage directory
after reading.

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
            "tempBuketPath": "gs://bucketName.datasetName/tableName",
            "jsonFilePath": "/path/to/jsonkeyfile",
            "importQuery":"SELECT TOP(corpus, 10) as title, COUNT(*) as
             unique_words FROM [publicdata:samples.shakespeare]",
            "InputTableId": "projectName:datasetName.tableName",
            "outputSchema": "title:string,unique_words:int"
          }
        }
      }