# Azure Blob Store Batch Source

Description
-----------

Batch source to use Azure Blob Store as a source.

Use Case
--------

This source is used whenever you need to read from a Azure Blob Store.
For example, you may want to read in files from Azure Blob Storage, parse them and then store
them in a Microsoft SQL Server Database.

Properties
----------

**path:** The path on Azure Blob Storage to use as input.

**account:** The Microsoft Azure Storage account to use.

**container:** The container to use on the specified Microsoft Azure Storage account.

**storageKey:** The storage key for the specified container on the specified Azure Storage account. Must be a valid
base64 encoded storage key provided by Microsoft Azure.

Example
-------

This example connects to Azure Blob Store and reads in files found in the specified directory. This example uses
the 'hydrator' container on the 'hydratorstorage.blob.core.windows.net' account on Azure Blob Store:

    {
        "name": "AzureBlobStore",
        "properties": {
            "path": "/path/to/input",
            "account": "hydratorstorage.blob.core.windows.net",
            "container": "hydrator",
            "storageKey": "XXXXXEEESSS/YYYY="
        }
    }
