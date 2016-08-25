# XML Reader Batch Source


Description
-----------
The XML Reader plugin is a source plugin that allows users to read XML files stored on HDFS.


Use Case
--------
A user would like to read XML files that have been dropped into HDFS.
These can be small to very large XML files. The XMLReader will read and parse the files,
and when used in conjunction with the XMLParser plugin, fields can be extracted.
This reader emits one XML event, specified by the node path property, for each file read.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**path:** Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'. (Macro-enabled)

**nodePath:** Node path to emit as an individual event from the XML schema.
Example: '/book/price' to read only the price from under the book node. (Macro-enabled)

**pattern:** Pattern to select specific file(s). (Optional) (Macro-enabled)
Examples:

1. Use '^' to select files with names starting with 'catalog', such as '^catalog'.
2. Use '$' to select files with names ending with 'catalog.xml', such as 'catalog.xml$'.
3. Use '\*' to select file with name contains 'catalogBook', such as 'catalogBook*'.

**actionAfterProcess:** Action to be taken after processing of the XML file.
Possible actions are:

1. Delete from the HDFS;
2. Archived to the target location; and
3. Moved to the target location.

**targetFolder:** Target folder path if user select action after process, either ARCHIVE or MOVE.
Target folder must be an existing directory. (Optional) (Macro-enabled)

**reprocessingRequired:** Specifies whether the file(s) should be reprocessed.

**tableName:** Table name to be used to keep track of processed file(s). (Macro-enabled)

**tableExpiryPeriod:** Expiry period (days) for data in the table. Default is 30 days.
Example: For tableExpiryPeriod = 30, data before 30 days get deleted from the table. (Macro-enabled)

**temporaryFolder:** An existing HDFS folder path with read and write access for the current user;
required for storing temporary files containing paths of the processed XML files.
These temporary files will be read at the end of the job to update the file track table.
Default to /tmp. (Macro-enabled)

Example
-------
This example reads data from the folder "hdfs:/cask/source/xmls/" and emits XML records on the basis of the node path
"/catalog/book/title". It will generate structured records with fields 'offset', 'fileName', and 'record'.
It will move the XML files to the target folder "hdfs:/cask/target/xmls/" and update the processed file information
in the table named "trackingTable".

      {
         "name": "XMLReaderBatchSource",
         "plugin":{
                    "name": "XMLReaderBatchSource",
                    "type": "batchsource",
                    "properties":{
                                  "referenceName": "referenceName""
                                  "path": "hdfs:/cask/source/xmls/",
                                  "Pattern": "^catalog"
                                  "nodePath": "/catalog/book/title"
                                  "actionAfterProcess" : "Move",
                                  "targetFolder":"hdfs:/cask/target/xmls/",
                                  "reprocessingRequired": "No",
                                  "tableName": "trackingTable",
                                  "temporaryFolder": "hdfs:/cask/tmp/"
                    }
         }
      }


 For this XML as an input:

     <catalog>
       <book id="bk104">
         <author>Corets, Eva</author>
         <title>Oberon's Legacy</title>
         <genre>Fantasy</genre>
         <price><base>5.95</base><tax><surcharge>13.00</surcharge><excise>13.00</excise></tax></price>
         <publish_date>2001-03-10</publish_date>
         <description><name><name>In post-apocalypse England, the mysterious
         agent known only as Oberon helps to create a new life
         for the inhabitants of London. Sequel to Maeve
         Ascendant.</name></name></description>
       </book>
       <book id="bk105">
         <author>Corets, Eva</author>
         <title>The Sundered Grail</title>
         <genre>Fantasy</genre>
         <price><base>5.95</base><tax><surcharge>14.00</surcharge><excise>14.00</excise></tax></price>
         <publish_date>2001-09-10</publish_date>
         <description><name>The two daughters of Maeve, half-sisters,
         battle one another for control of England. Sequel to
         Oberon's Legacy.</name></description>
       </book>
     </catalog>

 The output records will be:

    +==================================================================================+
    | offset | filename                            | record                            |
    +==================================================================================+
    | 2      | hdfs:/cask/source/xmls/catalog.xml  | <title>Oberon's Legacy</title>    |
    | 13     | hdfs:/cask/source/xmls/catalog.xml  | <title>The Sundered Grail</title> |
    +==================================================================================+
