# Oracle Export Action


Description
-----------
A Hydrator Action plugin to efficiently export data from Oracle to HDFS or local file system. The plugin uses
Oracle's command line tools to export data. The data exported from this tool can then be used in Hydrator pipelines.


Use Case
--------
A Hydrator user would like to export oracle data onto HDFS or local file system using an action plugin
that exports Oracle data using the Oracle bulk export tool as opposed to using JDBC.


Properties
----------

**oracleServerHostname:** Hostname of the remote Oracle Host.

**oracleServerSSHPort:** Port to use to SSH to the remote Oracle Host. Defaults to 22.

**oracleServerSSHUsername:** Username to use to connect to the remote Oracle Host via SSH.

**oracleServerSSHAuthMechanism :** Mechanism to perform the secure shell action. Acceptable values are Private Key,
Password.

**oracleServerSSHPassword:** Password to use to connect to the remote Oracle Host via SSH. This will be ignored
when 'Private Key' is used as the authentication mechanism.

**oracleServerSSHPrivateKey:** The private key to be used to perform the secure shell action. This will be ignored
when 'Password' is used as the authentication mechanism.

**passphrase:** Passphrase used to decrypt the provided private key in "privateKey". This will be ignored
when 'Password' is used as the authentication mechanism.

**dbUsername:** Username to connect to the Oracle database.

**dbPassword:** Password to connect the Oracle database.

**oracleHome:** Absolute path of the ``ORACLE_HOME`` environment variable on the Oracle server host.
This will be used to run the Oracle Spool utility.

**oracleSID:** Oracle System ID(SID). This is used to uniquely identify a particular database on the system.

**queryToExecute:** Query to be executed to export.For example: ``select * from test where name='cask'``;

**tmpSQLScriptFile:** Full path of the local temporary SQL script file which needs to be created.
It will be removed once the SQL command is executed.  Default is /tmp/tmpHydrator.sql. If 'commandToRun' input is used,
then filename has to be as same as in 'commandToRun'.

**commandToRun:** Oracle command to be executed on the Oracle host. When not provided, plugin will create
the command based on the input values provided in 'dbUsername', 'dbPassword', 'oracleHome', 'oracleSID',
'queryToExecute' and 'tmpSQLScriptDirectory'.
Format should be oracleHome/bin/sqlplus -s dbUsername/dbPassword@oracleSID @tmpSQLScriptFile

**outputPath:** Hdfs or local filesystem path where output file will be exported.

**format:** Format of the output file. Acceptable values are CSV, TSV, PSV.


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
              "oracleServerSSHPort": "22",
              "oracleServerSSHUsername": "oracle",
              "oracleServerSSHAuthMechanism": "Password",
              "oracleServerSSHPassword": "oracle@123",
              "oracleServerSSHPrivateKey": "",
              "oracleServerSSHPassphrase": "",
              "dbUsername": "system",
              "dbPassword": "cask",
              "oracleHome": "/u01/app/oracle/product/11.2.0/xe",
              "oracleSID": "cask",
              "queryToExecute": "select * from test where name='cask';",
              "tmpSQLScriptFile": "/tmp/tmpHydrator.sql",
              "commandToRun": "",
              "outputPath" : "/tmp/data.csv",
              "format" : "CSV"
          }
    }