# Oracle Export Action


Description
-----------
A plugin that can efficiently export data from Oracle to be used in Hydrator pipelines.
Oracle includes command line tools to export data that can be utilized to perform this task.


Use Case
--------
A Hydrator user would like to export oracle data onto hdfs or local file system using an action plugin
that does not require a JDBC connection to perform the export from Oracle.


Properties
----------

**oracleServerHostname:** Hostname of the remote DB machine.

**oracleServerPort:** Port of the remote DB machine.Defaults to 22.

**oracleServerUsername:** Username for remote DB host.

**oracleServerPassword:** Password for remote DB host.

**dbUsername:** Username to connect to oracle DB.

**dbPassword:** Password to connect to oracle DB.

**oracleHome:** Path of the ORACLE_HOME.

**oracleSID:** Oracle SID.

**queryToExecute:** Query to be executed to export.Query should have ';' at the end.

**pathToWriteFinalOutput:** Path where output file to be exported.

**format:** Format of the output file.


Example
-------
This example exports data from oracle server example.com using oracle user:

    {
        "name": "OracleExportAction",
          "plugin": {
            "name": "OracleExportAction",
            "type": "action",
            "label": "OracleExportAction",
            "artifact": {
              "name": "core-plugins",
              "version": "1.4.0-SNAPSHOT",
              "scope": "SYSTEM"
          },
          "properties": {
              "oracleServerHostname": "example.com",
              "oracleServerPort": "22",
              "oracleServerUsername": "oracle",
              "oracleServerPassword": "oracle@123",
              "dbUsername": "system",
              "dbPassword": "cask",
              "oracleHome": "/u01/app/oracle/product/11.2.0/xe",
              "oracleSID": "cask",
              "queryToExecute": "select * from test where name='cask';"
              "pathToWriteFinalOutput" : "/tmp/data.csv"
              "format" : "csv"
          }
    }