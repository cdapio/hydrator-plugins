# Oracle Export Action


Description
-----------
A Hydrator Action plugin to efficiently export data from Oracle to HDFS or local file system.
The plugin uses Oracle's command line tools to export data.
The data exported from this tool can then be used in Hydrator pipelines.


Use Case
--------
A Hydrator user would like to export oracle data onto HDFS or local file system using an action plugin
that exports Oracle data using the Oracle bulk export tool as opposed to using JDBC.


Properties
----------

**oracleServerHostname:** Hostname of the remote Oracle Host.

**oracleServerPort:** Port of the remote Oracle Host. Defaults to 22.

**oracleServerUsername:** Username to use to connect to the remote Oracle Host via SSH.

**oracleServerPassword:** Password to use to connect to the remote Oracle Host via SSH.

**dbUsername:** Username to connect to Oracle.

**dbPassword:** Password to connect to Oracle.

**oracleHome:** Absolute path of the ORACLE_HOME environment variable on the Oracle server host.This will be used to run
                the Oracle Spool utility.

**oracleSID:** Oracle System ID(SID). This is used to uniquely identify a particular database on the system.

**queryToExecute:** Query to be executed to export.

**pathToWriteFinalOutput:** Path where output file will be exported.

**format:** Format of the output file.Acceptable values are csv, tsv, psv.


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
              "version": "1.5.0-SNAPSHOT",
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