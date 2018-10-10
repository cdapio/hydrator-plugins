# KeyValueTable Batch Source


Description
-----------
Reads the entire contents of a KeyValueTable, outputting records with a 'key' field and a
'value' field. Both fields are of type bytes.


Use Case
--------
The source is used whenever you need to read from a KeyValueTable in batch. For example,
you may want to periodically dump the contents of a KeyValueTable to a Table.


Properties
----------
**Table Name:** Name of the KeyValueTable to read from. If the table does not already exist, it will be created. (Macro-enabled)


Example
-------
This example reads from a KeyValueTable named 'items':

    {
        "name": "KVTable",
        "type": "batchsource",
        "properties": {
            "name": "items"
        }
    }

It outputs records with this schema:

    +====================+
    | field name | type  |
    +====================+
    | key        | bytes |
    | value      | bytes |
    +====================+
