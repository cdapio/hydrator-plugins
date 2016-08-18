# FTP Batch Source


Description
-----------
Batch source for an FTP or SFTP source. Prefix of the path ('ftp://...' or 'sftp://...') determines the source server
type, either FTP or SFTP.


Use Case
--------
This source is used whenever you need to read from an FTP or SFTP server.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**path:** Path to file(s) to be read. Path is expected to be of the form prefix://username:password@hostname:port/path (Macro-enabled)

**fileRegex:** Regex to filter out filenames in the path. (Macro-enabled)

**fileSystemProperties:** A JSON string representing a map of properties
needed for the distributed file system. (Macro-enabled)

**inputFormatClassName:** Name of the input format class, which must be a subclass of FileInputFormat.
Defaults to CombineTextInputFormat. (Macro-enabled)


Example
-------
This example connects to an SFTP server and reads in files found in the specified directory.

    {
        "name": "FTP",
        "type": "batchsource",
        "properties": {
            "path": "sftp://username:password@hostname:21/path/to/logs"
        }
    }
