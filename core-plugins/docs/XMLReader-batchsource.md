# XMLReader Batch Source


Description
-----------
The XML Reader plugin is a source plugin that allows users to read XML files stored on HDFS.


Use Case
--------
Customer XML feeds that are dropped onto HDFS. These can be small to very large XML  files. The files have to read,
parsed and when used in conjunction with XML Parser they are able to extract fields. This reader will emit one XML
event that is specified by a Node Path to be extracted from the file.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**path:** Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'.

**nodePath:** Node path to emit individual event from the schema.
Example - '/book/price' to read only price under the book node

**pattern:** Pattern to select specific file(s).
Example -
1. Use '^' to select file with name start with 'catalog', like '^catalog'.
2. Use '$' to select file with name end with 'catalog.xml', like 'catalog.xml$'.
3. Use '*' to select file with name contains 'catalogBook', like 'catalogBook*'.

**actionAfterProcess:** Action to be taken after processing of the XML file.
Possible actions are -
1. Delete from the HDFS.
2. Archived to the target location.
3. Moved to the target location.

**targetFolder:** Target folder path if user select action after process, either ARCHIVE or MOVE. Target folder must be
an existing directory.

**reprocessingRequired:** Flag to know if file(s) to be reprocessed or not.

**tableName:** Table name to keep track of processed file(s).

**temporaryFolder** Existing hdfs folder path having read and write access to the current User, required for internal
computation of the plugin. Default value is /tmp.

Example
-------
This example reads data from folder "/source/xmls/" and emits XML records on the basis of node path
"/catalog/book/title". It will generate structured record with schema as 'offset', 'fileName', and 'record'.
It will move the XML file to the target folder "/target/xmls/" and update processed file information in trackingTable.

      {
         "name": "XMLReaderBatchSource",
         "plugin":{
                    "name": "XMLReaderBatchSource",
                    "type": "batchsource",
                    "properties":{
                                  "referenceName": "referenceName""
                                  "path": "/source/xmls/",
                                  "Pattern": "^catalog"
                                  "nodePath": "/catalog/book/title"
                                  "actionAfterProcess" : "Move",
                                  "targetFolder":"/target/xmls/", //this must be existing folder path
                                  "reprocessingRequired": "No",
                                  "tableName": "trackingTable",
                                  "temporaryFolder": "/tmp"
                    }
         }
      }


 For below XML as input file:

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

 Output schema will be:

    +==================================================================================+
    | offset | filename                            | record                            |
    +==================================================================================+
    | 2      | hdfs:/cask/source/xmls/catalog.xml  | <title>Oberon's Legacy</title>    |
    | 13     | hdfs:/cask/source/xmls/catalog.xml  | <title>The Sundered Grail</title> |
    +==================================================================================+
