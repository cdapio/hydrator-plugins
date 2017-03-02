# Refreshable Table Streaming Source


Description
-----------
Outputs the entire contents of a CDAP Table each batch interval. The Table contents will be refreshed
at configurable intervals.


Use Case
--------
This source is primarily used when you want to join it with data from another source. For example, the
Table may contain a mapping from a user id to a user email, and you want to enrich records from another
source with user email.


Properties
----------
**name:** The name of the table to read. (Macro-enabled)

**schema:** The schema to use when reading from the table. If the table does not already exist, one will be
created with this schema, which will allow the table to be explored through CDAP.

**rowField:** Optional schema field whose value is derived from the Table row instead of from a Table column.
The field name specified must be present in the schema, and must not be nullable.

**refreshInterval:** How often the table contents should be refreshed. Must be specified by a number followed by
a unit where 's', 'm', 'h', and 'd' are valid units corresponding to seconds, minutes, hours, and days. Defaults to '1h'.


Example
-------
This example reads the 'users' CDAP table and outputs the entire contents each batch interval. The table contents
are refreshed every six hours.

    {
        "name": "RefreshableTable",
        "type": "streamingsource",
        "properties": {
            "name": "users",
            "refreshInterval": "6h",
            "rowField": "id",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"event\",
                \"fields\":[
                    {\"name\":\"id\",\"type\":\"long\"},
                    {\"name\":\"name\",\"type\":\"string\"},
                    {\"name\":\"email\",\"type\":\"string\"}
                ]
            }"
        }
    }
