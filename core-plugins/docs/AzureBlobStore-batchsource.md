# Microsoft Azure Blob Storage Batch Source

Description
-----------

Batch source to use Microsoft Azure Blob Storage as a source.

Use Case
--------

This source is used whenever you need to read from Microsoft Azure Blob Storage. For
example, you may want to read in files from Microsoft Azure Blob Storage, parse them and
then store them in a Microsoft SQL Server Database.

Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**path:** The path on Microsoft Azure Blob Storage to use as input. The path uses filename expansion (globbing) to read
files. The path must start with `wasb://` or `wasbs://`, for example, `wasb://mycontainer@mystorageaccount.blob.core.windows.net/filename.txt`. (Macro-enabled)

**account:** The Microsoft Azure Blob Storage account to use. This is of the form 
`mystorageaccount.blob.core.windows.net`, where `mystorageaccount` is the Microsoft 
Azure Storage account name. (Macro-enabled)

**storageKey:** The storage key for the container on the Microsoft Azure Storage account. 
Must be a valid base64 encoded storage key provided by Microsoft Azure. (Macro-enabled)

**ignoreNonExistingFolders:** Identify if path needs to be ignored or not, for case when directory or file does not
exists. If set to true it will treat the not present folder as 0 input and log a warning. Default is false.

**recursive:** Boolean value to determine if files are to be read recursively from the path. Default is false.

Example
-------

This example connects to Microsoft Azure Blob Storage and reads in files found in the
specified directory. This example uses Microsoft Azure Storage 'mystorageaccount.blob.core.windows.net', using the
'mystorageaccount' account name:

    {
        "name": "AzureBlobStore",
        "type": "batchsource",
        "properties": {
            "path": "`wasb://mycontainer@mystorageaccount.blob.core.windows.net/filename.txt",
            "account": "mystorageaccount.blob.core.windows.net",
            "storageKey": "XXXXXEEESSS/YYYY=",
            "ignoreNonExistingFolders": "false",
            "recursive": "false"
        }
    }
