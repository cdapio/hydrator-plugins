# S3 File Metadata Batch Source

Description
-----------
The S3 File Metadata plugin is a source plugin that allows users to read file metadata from an S3 Filesystem.


Use Case
--------
Use this source to extract the metadata of files under specified paths. The metadata can be passed to the
HDFSFileCopySink and the files will be copied from S3 to HDFS


Properties
----------
| Configuration          | Required | Default   | Description                                                                                                                                                                                                                                                                     |
| :--------------------- | :------: | :------   | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Reference Name**     |  **Y**   | None      | This will be used to uniquely identify this source for lineage, annotating metadata, etc.                                                                                                                                                                                       |
| **Source Paths**       |  **Y**   | None      | Path(s) to file(s) to be read. If a directory is specified, terminate the path name with a '/'.                                                                                                                                                                                 |
| **Max Split Size**     |  **Y**   | None      | Specifies the number of files that are controlled by each split. The number of splits created will be the total number of files divided by Max Split Size. The InputFormat will assign roughly the same number of bytes to each split.                                          |
| **Copy Recursively**   |  **Y**   | True      | Whether or not to copy recursively. Similar to the '-r' option in the linux cp command. Set this to true if you want to copy the entire directory.                                                                                                                              |
| **Access Key ID**      |  **Y**   | None      | Your Amazon S3 access key ID.                                                                                                                                                                                                                                                   |
| **Secret Key ID**      |  **Y**   | None      | Your Amazon S3 secret key ID.                                                                                                                                                                                                                                                   |
| **Bucket Name**        |  **Y**   | None      | Address of the bucket which you want to read from.                                                                                                                                                                                                                              |
| **Amazon Region**      |  **N**   | us-east-1 | Region of the bucket which you want to read from.                                                                                                                                                                                                                               |

Usage Notes
-----------
This source plugin only reads filemetadata from the source. To copy files, pass its outputs to a FileCopySink of the desired Filesystem.
A structured record with the following schema is emitted for each file it reads.

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
| **Access Key ID**      | String | Access Key ID for the source Filesystem.                                                                                                       |
| **Secret Key ID**      | String | Secret Key ID for the source Filesystem.                                                                                                       |
| **Bucket Name**        | String | The bucket which this file came from.                                                                                                          |
