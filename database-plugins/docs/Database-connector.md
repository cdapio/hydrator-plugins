# Database Connector


Description
-----------
This plugin can be used to browse and sample data from relational database using JDBC. This is a generic database
connector plugin which may not handle some special case for certain special databases that have specific implementation
of JDBC. Try to use those specific database connector plugins (e.g. oracle connector plugin) if they exist.



Properties
----------
**Plugin Name:** Name of the JDBC plugin to use. This is the value of the 'name' key
defined in the JSON file for the JDBC plugin.

**Connection String:** JDBC connection string including database name.

**Username:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**Password:** Password to use to connect to the specified database. Required for databases
that need authentication. Optional for databases that do not require authentication.

**Connection Arguments:** A list of arbitrary string tag/value pairs as connection arguments. These arguments
will be passed to the JDBC driver, as connection arguments, for JDBC drivers that may need additional configurations.
This is a semicolon-separated list of key-value pairs, where each pair is separated by a equals '=' and specifies
the key and value for the argument. For example, 'key1=value1;key2=value' specifies that the connection will be
given arguments 'key1' mapped to 'value1' and the argument 'key2' mapped to 'value2'.