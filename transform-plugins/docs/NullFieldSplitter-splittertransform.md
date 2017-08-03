# Null Field Splitter


Description
-----------
This plugin is used when you want to split records based on whether a specific field is null or not.
Records with a null value for the field are sent to the ``null`` port while records with a non-null
value are sent to the ``nonnull`` port.

Properties
----------
**field:** Specifies which field should be checked for null values. (Macro-enabled)

**modifySchema:** Whether to modify the schema for non-null output.
If set to true, the schema for non-null output will be modified so that the field is no longer nullable.
Defaults to true.

Example
-------

    {
        "name": "NullFieldSplitter",
        "type": "splittertransform"
        "properties": {
            "field": "email",
            "modifySchema": "true"
        }
    }

This example takes splits input based on whether the ``email`` field is null.
For example, if the input to the plugin is:

     +======================================================+
     | id (long) | name (string) | email (nullable string)  |
     +======================================================+
     | 0         | alice         |                          |
     | 1         | bob           |                          |
     | 2         | carl          | karl@example.com         |
     | 3         | duncan        | duncandonuts@example.com |
     | 4         | evelyn        |                          |
     | 5         | frank         | frankfurter@example.com  |
     | 6         | gary          | gerry@example.com        |
     +======================================================+

then records emitted to the ``null`` port will be:

     +======================================================+
     | id (long) | name (string) | email (nullable string)  |
     +======================================================+
     | 0         | alice         |                          |
     | 1         | bob           |                          |
     | 4         | evelyn        |                          |
     +======================================================+

and records emitted to the ``nonnull`` port will be:

     +======================================================+
     | id (long) | name (string) | email (string)           |
     +======================================================+
     | 2         | carl          | karl@example.com         |
     | 3         | duncan        | duncandonuts@example.com |
     | 5         | frank         | frankfurter@example.com  |
     | 6         | gary          | gerry@example.com        |
     +======================================================+
