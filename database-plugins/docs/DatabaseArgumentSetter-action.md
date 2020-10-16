# Database Argument Setter Action


Description
-----------
Action that converts table column into pipeline arguments.


Use Case
--------
The action can be used whenever you want to pass data from database as arguments to a data pipeline.
For example, you may want to read a particular table at runtime to set arguments for the pipeline.


Properties
----------
**Driver Name:** Name of the JDBC driver to use.

**Connection String:** JDBC connection string including database name.

**Database:** Database name.

**Table:** Table name.

**Argument selection conditions:** A set of conditions for identifying the arguments to run a pipeline. Multiple conditions can be specified in the format
                                   column1=<column1-value>;column2=<column2-value>.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Arguments column:** Name of the column that contains the arguments.

Example
-------
Suppose you have a configuration management database and you want to read a particular table at runtime to set arguments for the pipeline, including the source configuration, the transformations, the sink configuration, etc. 

```
Driver Name: "postgres"
Connection String: "jdbc:postgresql://localhost:5432/configuration"
Database: "configuration"
Table: "files"
Argument selection conditions: "file_id=1"
Arguments column: "file_name"
```