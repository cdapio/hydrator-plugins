# Windows share copy action


Description
-----------
Copies a file or files on a windows share to HDFS directory


Use Case
--------
This action can be used when a file or files need to be moved from windows share to HDFS directory.


Properties
----------
**netBiosDomainName:** Specifies the NetBios domain name. Default is null. For example: `MyDomain`

**netBiosHostname:** Specifies the NetBios hostname to import files from. For example: `10.150.10.10`

**netBiosUsername:** Specifies the NetBios username to user for importing files from share.

**netBiosPassword:** Specifies the NetBios password for importing files from windows share.

**netBiosSharename:** Specifies the NetBios share name. For example: `share`

**numThreads:** Specifies the number of parallel tasks to execute the copy operation. Defaults to 1.

**sourceDirectory:** Specifies the NetBios directory.For example: `/source/`

**destinationDirectory:** The valid, full HDFS destination path in the same cluster where the file or files are to be
moved. If a directory is specified with a file sourcePath, the file will be put into that directory. If sourcePath is a
directory, it is assumed that destPath is also a directory. HDFSAction will not catch this inconsistency.

**bufferSize:** The size of the buffer to be used for copying the files. Minimum Buffer size is 4096. Default is 4096.

**overwrite:** Boolean that specifies if the files already present should be overwritten or not. Default is true.

Example
-------
This example copies all the files from `10.150.10.10:/source/share/path` to `/dest/path`:

    {
        "name": "WindowsShareCopy",
        "plugin": {
            "name": "HDFSMove",
            "type": "action",
            "artifact": {
                "name": "core-plugins",
                "version": "1.4.1-SNAPSHOT",
                "scope": "SYSTEM"
            },
            "properties": {
                "netBiosDomainName": "domain",
                "netBiosHostname": "10.150.10.10",
                "netBiosUsername": "username",
                "netBiosPassword": "password",
                "netBiosSharename": "share",
                "sourceDirectory": "source/",
                "destinationDirectory": "hdfs://example.com/dest/path",
                "bufferSize": "4096",
                "numThreads": "5"
            }
        }
    }
