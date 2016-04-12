===========
Url Plugins
===========

Introduction
============

A Hydrator Real Time Plugin that allows data to be fetched from a given Url at a specified interval.

Getting Started
===============

Prerequisites
-------------

To use this plugin, you must have CDAP version 3.4.0 or later.

Build
-----
To build your plugins::

  mvn clean package -DskipTests

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

UI Integration
--------------
The Cask Hydrator UI displays each plugin property as a simple textbox. To customize how the plugin properties
are displayed in the UI, you can place a configuration file in the ``widgets`` directory.
The file must be named following a convention of ``[plugin-name]-[plugin-type].json``.
See [Plugin Widget Configuration](http://docs.cdap.io/cdap/current/en/cdap-apps/hydrator/custom.html#plugin-configuration-json)
for details on the configuration file.
The UI will also display a reference doc for your plugin if you place a file in the ``docs`` directory
that follows the convention of ``[plugin-name]-[plugin-type].md``.

When the build runs, it will scan the ``widgets`` and ``docs`` directories in order to build an appropriately
formatted .json file under the ``target`` directory. This file is deployed along with your .jar file to add your
plugins to CDAP.

Deployment
----------
You can deploy your plugins using the CDAP CLI::

  > load artifact <target/plugin.jar> config-file <target/plugin.json>

For example, if your artifact is named 'my-plugins-1.0.0'::

  > load artifact target/my-plugins-1.0.0.jar config-file target/my-plugins-1.0.0.json


License and Trademarks
======================

Copyright © 2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.