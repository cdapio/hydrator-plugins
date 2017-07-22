# HDfS File Copy Batch Sink

Description
-----------
The HDFS File Copy plugin is a sink plugin that takes file metadata records as inputs and copies the files into local HDFS


Use Case
--------
Use this sink to copy files from any source to your local HDFS.
You may want to periodically sync your HDFS with some remote filesystem. Schedule to run a pipeline with this plugin to copy new files periodically.


Properties
----------
| Configuration          | Required | Default   | Description                                                                                 |
| :--------------------- | :------: | :------   | :------------------------------------------------------------------------------------------ |
| **Reference Name**     |  **Y**   | None      | This will be used to uniquely identify this source for lineage, annotating metadata, etc.   |
| **Base Path**          |  **Y**   | None      | The folder where the copied files will be placed. It will be created if it doesn't exist.   |
| **Enable Overwrite**   |  **Y**   | False     | Specifies whether or not to overwrite files if it already exists.                           |
| **Buffer Size**        |  **Y**   | True      | The size of the buffer that temporarily stores data from file input stream while copying.   |

Usage Notes
-----------
This sink plugin only reads StructuredRecords with the follwing schema. Each record should contain the metadata for a file to be copied

| Field                  | Type   | Description                                                                                                                                    |
| :--------------------- | :----- | :-------------------------                                                                                                                     |
| **File Name**          | String | Only contains the name of the file.                                                                                                            |
| **Full Path**          | String | Contains the full path of the file in the source file system.                                                                                  |
| **File Size**          | long   | Specifies the number of fi                                                                                                                     |
| **Timestamp**          | long   | The modification timestamp of the file.                                                                                                        |
| **Owner**              | String | The owner of the file.                                                                                                                         |
| **Is Folder**          | Boolean| Whether or not the file is a folder.                                                                                                           |
| **Base Path**          | String | The base path of the file. This path will be appended to the base path specified in the FileCopySink to create the file in the destination.    |
| **Data Base Type**     | String | Contains the string "amazons3". Used to identify the type of filesystem this record originated from.                                           |
| **Credentials**        | Record | Additional information required to connect to the source Filesystem.                                                                           |
