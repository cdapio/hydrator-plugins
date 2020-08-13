# File Streaming Source


Description
-----------
File streaming source. Watches a directory and streams file contents of any new files added to the directory.
Files must be atomically moved or renamed.


Use Case
--------
This source is used whenever you want to read files from a directory in a streaming context.
For example, you may want to read access logs as they are moved into an archive directory.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**path:** The path of the directory to monitor. Files must be written to the monitored directory by
"moving" them from another location within the same file system. File names starting with . are ignored. (Macro-enabled)

**format:** Format of files in the directory. Supported formats are 'text', 'csv', 'tsv', 'clf', 'grok', and 'syslog'.
The default format is 'text'. (Macro-enabled)

**schema:** Schema of files in the directory.

**extensions:** Comma separated list of file extensions to accept. If not specified, all files in the directory
will be read. Otherwise, only files with an extension in this list will be read. (Macro-enabled)

**ignoreThreshold:** Ignore files that are older than this many seconds. Defaults to 60. (Macro-enabled)


Example
-------
This example reads from the '/data/events' directory on an hdfs cluster whose namenode
is running on namenode.example.com. The source will monitor the directory for any
new files that are added. It will parse those files as comma separated files with three
columns: timestamp, user, and action.

    {
        "name": "File",
        "type": "streamingsource",
        "properties": {
            "path": "hdfs://namenode.example.com/data/events",
            "format": "csv",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"event\",
                \"fields\":[
                    {\"name\":\"timestamp\",\"type\":\"long\"},
                    {\"name\":\"user\",\"type\":\"string\"},
                    {\"name\":\"action\",\"type\":\"string\"}
                ]
            }"
        }
    }
