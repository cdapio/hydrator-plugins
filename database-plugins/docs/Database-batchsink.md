# Database Batch Sink


Description
-----------
Writes records to a database table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a database table.
For example, perhaps you periodically build a recommendation model for products on your online store.
The model could be stored in a FileSet and you may want to export the contents
of the FileSet to a database table where it can be served to your users.


Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**tableName:** Name of the table to export to. (Macro-enabled)

**columns:** Comma-separated list of columns in the specified table to export to.

**columnCase:** Sets the case of the column names returned by the column check query.
Possible options are ``upper`` or ``lower``. By default, or for any other input, the column names are not modified and
the names returned from the database are used as-is. Note that setting this property provides predictability
of column name cases across different databases but might result in column name conflicts if multiple column
names are the same when the case is ignored (optional).

**connectionString:** JDBC connection string including database name. (Macro-enabled)

**user:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication. (Macro-enabled)

**password:** Password to use to connect to the specified database. Required for databases
that need authentication. Optional for databases that do not require authentication. (Macro-enabled)

**jdbcPluginName:** Name of the JDBC plugin to use. This is the value of the 'name' key
defined in the JSON file for the JDBC plugin.

**jdbcPluginType:** Type of the JDBC plugin to use. This is the value of the 'type' key
defined in the JSON file for the JDBC plugin. Defaults to 'jdbc'.

**enableAutoCommit:** Whether to enable auto-commit for queries run by this sink. Defaults to 'false'.
This setting only matters if you are using a JDBC driver -- such as the Hive driver -- 
that will error when the commit operation is run, or a driver that will error when auto-commit is
set to `false`. For drivers like these, you will need to set this to `true`.

**schema:** The schema of records output by the source. This will be used in place of the schema that comes
back from the query. However, it must match the schema that comes back from the query,
except that it can mark fields as nullable and can contain a subset of the fields.

**transactionIsolationLevel:** The transaction isolation level at which operations will be made. See
[Wikipedia](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Isolation_levels) for a detailed explanation.


Example
-------
This example connects to a database using the specified 'connectionString', which means
it will connect to the 'prod' database of a PostgreSQL instance running on 'localhost'.
Each input record will be written to a row of the 'users' table, with the value for each
column taken from the value of the field in the record. For example, the 'id' field in
the record will be written to the 'id' column of that row.

    {
        "name": "Database",
        "type": "batchsink",
        "properties": {
            "tableName": "users",
            "columns": "id,name,email,phone",
            "connectionString": "jdbc:postgresql://localhost:5432/prod",
            "user": "postgres",
            "password": "",
            "jdbcPluginName": "postgres",
            "jdbcPluginType": "jdbc",
            "transactionIsolationLevel": "Serializable"
        }
    }
