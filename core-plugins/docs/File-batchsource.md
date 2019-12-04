# File Batch Source


## Description

This source is used whenever you need to read from a distributed file system.
For example, you may want to read in log files from S3 every hour and then store
the logs in a TimePartitionedFileSet.


## Properties

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Path:** Path to read from. For example, s3a://<bucket>/path/to/input

**Format:** Format of the data to read.
The format must be one of 'avro', 'blob', 'csv', 'delimited', 'json', 'parquet', 'text', 'tsv' or 'orc'.
If the format is 'blob', every input file will be read into a separate record.
The 'blob' format also requires a schema that contains a field named 'body' of type 'bytes'.
If the format is 'text', the schema must contain a field named 'body' of type 'string'.

**Delimiter:** Delimiter to use when the format is 'delimited'. This will be ignored for other formats.

**Maximum Split Size:** Maximum size in bytes for each input partition.
Smaller partitions will increase the level of parallelism, but will require more resources and overhead.
The default value is 128MB.

**Path Field:** Output field to place the path of the file that the record was read from.
If not specified, the file path will not be included in output records.
If specified, the field must exist in the output schema as a string.

**Path Filename Only:** Whether to only use the filename instead of the URI of the file path when a path field is given.
The default value is false.

**Read Files Recursively:** Whether files are to be read recursively from the path. The default value is false.

**Allow Empty Input:** Whether to allow an input path that contains no data. When set to false, the plugin
will error when there is no data to read. When set to true, no error will be thrown and zero records will be read.

**File System Properties:** Additional properties in json format to use with the InputFormat when reading the data.


## How to get Output Schema?

Formats supported in File plugin can be categorised into - `hadoop` formats and `non-hadoop` formats

1. For `hadoop` file formats - `orc`, `parquet` :

   `GetSchema` button is provided in plugin's configuration UI to help user fetch the schema.
   If value provided in `Path` config is a directory then schema would be fetched from any random file picked from specified path
   matching extension `.orc` or `.parquet` (depending on selected `Format`).


2. For `non-hadoop` file formats - `csv`, `delimited`, `tsv`, `json`, `avro` :

   Pls use DataPrep to identify the
   schema of the file by applying `parse-as-<format>` directive or `Parse-><format>` on the `body` column.
   one can click on create pipeline, select batch, and then open wrangler stage and export the schema.
   Once exported, go back to file plugin, select `Format` as the case is and import this schema file.



## Note

It is mandatory to provide output schema when using format other than `text`. Default schema used in this plugin is for `text` format where body represents line read from the file and offset represent offset of line in the file.

If the format is `orc` then only string, long, int, double, float, boolean and array types are supported in output schema.

