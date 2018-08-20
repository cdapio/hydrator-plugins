# File Move Action


Description
-----------
Moves a file or files.


Use Case
--------
This action can be used when a file or files need to be moved to a new location in a file system,
often required when archiving files. The source and destination must be on the same file system.


Properties
----------
**sourcePath:** The full path of the file or directory that is to be moved. In the case of a directory, if
fileRegex is set, then only files in the source directory matching the wildcard regex will be moved.
Otherwise, all files in the directory will be moved. For example: `hdfs://hostname/tmp`.

**destPath:** The valid, full destination path in the same cluster where the file or files are to be moved.
If a directory is specified with a file sourcePath, the file will be put into that directory. If sourcePath is
a directory, it is assumed that destPath is also a directory.

**fileRegex:** Wildcard regular expression to filter the files in the source directory that will be moved.

**continueOnError:** Indicates if the pipeline should continue if the move process fails. If all files are not 
successfully moved, the action will not return the files already moved to their original locations.


Example
-------
This example moves a file from `/source/path` to `/dest/path`:

    {
        "name": "FileMove",
        "plugin": {
            "name": "FileMove",
            "type": "action",
            "artifact": {
                "name": "core-plugins",
                "version": "1.4.0-SNAPSHOT",
                "scope": "SYSTEM"
            },
            "properties": {
                "sourcePath": "hdfs://example.com:8020/source/path",
                "destPath": "hdfs://example.com:8020/dest/path",
                "fileRegex": ".*\.txt",
                "continueOnError": "false"
            }
        }
    }
