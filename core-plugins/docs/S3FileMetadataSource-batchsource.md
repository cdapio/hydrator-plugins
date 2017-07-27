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
| **Host URI**           |  **Y**   | None      | URI of the bucket which you want to read from. For example, if you want to read from s3 bucket `example.bucket.co`, this configuration would be `s3a://example.bucket.co`                                                                                                       |
| **Copy Recursively**   |  **Y**   | True      | Whether or not to copy recursively. Similar to the '-r' option in the linux cp command. Set this to true if you want to copy the entire directory.                                                                                                                              |
| **Access Key ID**      |  **Y**   | None      | Your Amazon S3 access key ID.                                                                                                                                                                                                                                                   |
| **Secret Key ID**      |  **Y**   | None      | Your Amazon S3 secret key ID.                                                                                                                                                                                                                                                   |
| **Amazon Region**      |  **N**   | us-east-1 | Region of the bucket which you want to read from.                                                                                                                                                                                                                               |

Usage Notes
-----------
This source plugin only reads filemetadata from the source. To copy files, pass its outputs to a FileCopySink of the desired Filesystem.
A structured record with the following schema is emitted for each file it reads.

| Field                  | Type   | Description                                                                                                                                    |
| :--------------------- | :----- | :-------------------------                                                                                                                     |
| **fileName**           | String | Only contains the name of the file.                                                                                                            |
| **fullPath**           | String | Contains the full path of the file in the source file system.                                                                                  |
| **fileSize**           | long   | File Szie in bytes.                                                                                                                            |
| **hostURI**            | String | The source filesystem's URI.                                                                                                                   |
| **modificationTime**   | long   | The modification timestamp of the file.                                                                                                        |
| **group**              | String | The group that the of the file belongs to.                                                                                                     |
| **owner**              | String | The owner of the file.                                                                                                                         |
| **isFolder**           | Boolean| Whether or not the file is a folder.                                                                                                           |
| **basePath**           | String | The base path of the file. This path will be appended to the base path specified in the FileCopySink to create the file in the destination.    |
| **filesystem**         | String | Contains the string "amazons3". Used to identify the type of filesystem this record originated from.                                           |
| **permission**         | int    | The file's access permission                                                                                                                   |
| **accessKeyID**        | String | Access Key ID for the source Filesystem.                                                                                                       |
| **secretKeyID**        | String | Secret Key ID for the source Filesystem.                                                                                                       |
| **region**             | String | Region of the source Filesystem.                                                                                                               |
