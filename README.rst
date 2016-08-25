===========================
Hydrator Plugins Collection
===========================

.. image:: https://travis-ci.org/caskdata/hydrator-plugins.svg?branch=develop
    :target: https://travis-ci.org/caskdata/hydrator-plugins

Introduction
============

The Cask™ Data Application Platform (CDAP) is an integrated, open source application
development platform for the Hadoop ecosystem that provides developers with data and
application abstractions to simplify and accelerate application development.

|(Hydrator)| 

Cask Hydrator is one such application built by the team at Cask for solving data ingestion 
problems. It's a CDAP native application that runs at scale on Hadoop. Hydrator exposes 
APIs that allows one to build, extend, and integrate with Hadoop native and non-Hadoop systems. 

This repository is a collection of Hydrator plugins that were built by the team at Cask and by 
people in the CDAP community. If you are interested in building and contributing a plugin, you are more 
than welcome to do so. If you would like to build plugins, here are links to get you started:

- `Overview of Hydrator <http://docs.cask.co/cdap/current/en/hydrator-manual/index.html>`__
- `How to build custom Hydrator plugins
  <http://docs.cask.co/cdap/current/en/hydrator-manual/developing-plugins/index.html>`__
- `How to test a plugin <http://docs.cask.co/cdap/current/en/hydrator-manual/developing-plugins/testing-plugins.html>`__
- `How to package a plugin 
  <http://docs.cask.co/cdap/current/en/hydrator-manual/developing-plugins/packaging-plugins.html>`__
- `How to deploy a plugin
  <http://docs.cask.co/cdap/current/en/hydrator-manual/plugin-management.html#deploying-plugins>`__
- `How to include (deploy) third-party JARs
  <http://docs.cask.co/cdap/current/en/hydrator-manual/plugin-management.html#deploying-third-party-jars>`__
- `CDAP Developers' Manual: Plugins <http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/plugins.html>`__


Getting Started
===============

Prerequisites
-------------
To use the 1.4.x version of Hydrator plugins, you must have CDAP version 3.5.x or higher. Prerequisites for the various
sources, sinks, and transforms are included in their individual README files.
  
Building Plugins
----------------
You get started with Hydrator plugins by building directly from the latest source code::

  git clone https://github.com/caskdata/hydrator-plugins.git
  cd hydrator-plugins
  mvn clean package

After the build completes, you will have a JAR for each plugin under each
``<plugin-name>/target/`` directory.

Deploying Plugins
-----------------
You can deploy a plugin using the CDAP CLI::

  > load artifact <target/plugin-jar> config-file <resources/plugin-config>

Example for loading the Cassandra Plugin (from the cassandra-plugins directory)::

  > load artifact target/cassandra-plugins-<version>.jar \
         config-file target/cassandra-plugins-<version>.json

You can build without running tests: ``mvn clean install -DskipTests``

Mailing Lists
-------------
CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

IRC Channel
-----------
CDAP IRC Channel: #cdap on irc.freenode.net


License and Trademarks
======================

Copyright © 2015-2016 Cask Data, Inc.

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

.. |(Hydrator)| image:: http://cask.co/wp-content/uploads/hydrator_logo_cdap1.png
