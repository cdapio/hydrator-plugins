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

**path:** The path on Microsoft Azure Blob Storage to use as input.

**account:** The Microsoft Azure Blob Storage account to use. This is of the form 
`mystorageaccount.blob.core.windows.net`, where `mystorageaccount` is the Microsoft 
Azure Storage account name.

**container:** The container to use on the specified Microsoft Azure Storage account.

**storageKey:** The storage key for the specified container on the Microsoft Azure Storage account. 
Must be a valid base64 encoded storage key provided by Microsoft Azure.

Example
-------

This example connects to Microsoft Azure Blob Storage and reads in files found in the
specified directory. This example uses the 'hydrator' container at
'hydratorstorage.blob.core.windows.net' (Microsoft Azure Storage), using the
'hydratorstorage' account name:

    {
        "name": "AzureBlobStore",
        "type": "batchsource",
        "properties": {
            "path": "/path/to/input",
            "account": "hydratorstorage.blob.core.windows.net",
            "container": "hydrator",
            "storageKey": "XXXXXEEESSS/YYYY="
        }
    }
