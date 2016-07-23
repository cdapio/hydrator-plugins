# File Move Action


Description
-----------
Establishes an SSH connection with desired destination machine and moves file from source remote machine to that
destination machine.


Use Case
--------
This action can be used to move data or files between remote machines for purposes such as archiving data before
the pipeline starts or to move data into HDFS before the pipeline starts.
Possible Source-Destination machine pairings: FTP->HDFS  FTP->Unix HDFS->HDFS  Unix->Unix  Unix-HDFS


Properties
----------
**sourceDestinationPair:** Identification for the type of source and destination machine the action will be dealing with.

**sourceHost:** The host name of the source remote machine where the file needs to be extracted.

**sourcePort:** The port of the source machine used to make SSH connection. Defaults to 22.

**sourceUser:** The username used to connect to the source hostname.

**sourcePrivateKeyFile:** The file path to the private key used to properly authenticate with the source hostname.

**sourcePassword:** The password associated with the source private key.

**sourceFile** The absolute path of the source file.

**destHost:** The host name of the destination remote machine where the file needs to be extracted.

**destPort:** The port of the destination machine used to make SSH connection. Defaults to 22.

**destUser:** The username used to connect to the destination hostname.

**destPrivateKeyFile:** The file path to the private key used to properly authenticate with the destination hostname.

**destPassword:** The password associated with the destination private key.

**destFile:** The absolute path of the desired location for the file.


Example
-------
This example moves file 'oldData.txt' from source@unix1.com to destination@unix2.com:

    {
        "name": "FileMoveAction",
          "plugin": {
            "name": "FileMoveAction",
            "type": "action",
            "label": "FileMoveAction",
            "artifact": {
              "name": "core-plugins",
              "version": "1.4.0-SNAPSHOT",
              "scope": "SYSTEM"
          },
          "properties": {
              "sourceDestinationPair": "Unix->Unix",
              "sourceHost": "unix1.com",
              "sourcePort": 22,
              "sourceUser": "source",
              "sourcePrivateKeyFile": "/path/to/dest/privateKey",
              "sourcePassword": "pass1",
              "sourceFile": "/source/file/oldData.txt",
              "destHost": "unix2.com",
              "destPort": 22,
              "destUser": "destination",
              "destPrivateKeyFile": "/path/to/dest/privateKey",
              "destPassword": "pass2"
              "destFile": "/destination/file/oldData.txt"
            }
          }
    }
