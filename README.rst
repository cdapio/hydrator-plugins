===========================
Hydrator Plugins Collection
===========================

.. image:: https://travis-ci.org/caskdata/hydrator-plugins.svg?branch=develop
    :target: https://travis-ci.org/caskdata/hydrator-plugins
    
.. image:: https://badges.ondemand.coverity.com/jobs/2mi4qrcv9h4v9btngg3d0mkhe0
    :target: https://ondemand.coverity.com/jobs/2mi4qrcv9h4v9btngg3d0mkhe0/results

Introduction
============

The Cask™ Data Application Platform (CDAP) is an integrated, open source application
development platform for the Hadoop ecosystem that provides developers with data and
application abstractions to simplify and accelerate application development.

|(Hydrator)| 

Cask Hydrator is one such application built by the team at Cask for solving data ingestion 
problems. It's a CDAP Native application that runs at scale on Hadoop. Hydrator exposes 
APIs that allows one to build, extend and integrate with Hadoop Native and Non-Hadoop systems. 

This repository is a collection of hydrator plugins that are built by team at Cask and as well as 
people in the community. If you are interested in building and contributing a plugins you are more 
than welcome to do so. If you are interested in getting started with building some plugins here 
are some useful links for you to get started

- `Overview of Hydrator <http://docs.cask.co/cdap/3.2.1/en/included-applications/etl/index.html>`__
- `How to build custom Hydrator plugins <http://docs.cask.co/cdap/3.2.1/en/included-applications/etl/custom.html>`__
- `How to test plugin <http://docs.cask.co/cdap/3.2.1/en/included-applications/etl/custom.html#test-framework-for-plugins>`__
- `How to package and deploy <http://docs.cask.co/cdap/3.2.1/en/included-applications/etl/custom.html#plugin-packaging-and-deployment>`__
- `How to include third-party JARs <http://docs.cask.co/cdap/3.2.1/en/included-applications/etl/plugins/third-party.html>`__


Getting Started
===============

Prerequisites
-------------

To use CDAP-plugins, you must have CDAP version 3.2.0 or later. Prerequisites for the various
sources and sinks are included in their individual README files.
  
Build Plugins
-------------

You can get started with CDAP-plugins by building directly from the latest source code::

  git clone https://github.com/caskdata/hydrator-plugins.git
  cd hydrator-plugins
  mvn clean package

After the build completes, you will have a jar for each plugin under the
``<plugin-name>/target/`` directory.

Deploy Plugins
--------------

You can deploy plugins using the CDAP CLI::

  > load artifact <target/plugin-jar> config-file <resources/plugin-config>

Example for loading Cassandra Plugin (from cassandra-plugins directory)::

  > load artifact target/cassandra-plugins-1.0-SNAPSHOT-batch.jar \
         config-file resources/cassandra-plugin-batch.json
  > load artifact target/cassandra-plugins-1.0-SNAPSHOT-realtime.jar \
         config-file resources/cassandra-plugin-realtime.json

Then copy the UI configuration into UI templates directory::

  $ cp resources/Cassandra.json $CDAP_HOME/ui/templates/common

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

.. |(Hydrator)| image:: http://cask.co/wp-content/uploads/hydrator_logo_cdap1.png
