# CopybookReader plugin


Description
-----------
This is a source plugin that would allow users to read and process mainframe files defined using COBOL Copybook.
This is the basic first implementation.

Use Case
--------

It's basically used for reading flat file or dataset that is generated on a z/OS IBM mainframe based on a fixed length
COBOL Copybook. This will also work on AS/400 computers.
So, if a customer has flat files on HDFS that can be parsed using simple COBOL Copybook, then applying the Copybook
one is able to read the file and it's fields easily.


Properties
----------

**CopybookContents :** Contents of the COBOL Copybook file which will contain the data structure
User will have to paste the entire contents of the Copybook file in the test area.
The Copybook contents should be in sync with the underlying binary data to be read.
First implementation does not handle complex nested structures of COBOL Copybook
Also it does not handle Redefines or iterators in the structure

**binaryFilePath   :** Complete path of the .bin to be read.This will be a fixed length binary format file,that
matches the Copybook.
Supports compressed files - Native Compressed Codec

**fileStructure    :** Copybook file structure. For the current implementation only fixed length flat files
will be read.
fixed length binary format is supported in the first implementation

**schema:** The schema for the data as it will be formatted in CDAP.


Example
-------

This example reads data from a local binary file "file:///home/cdap/cdap/DTAR020_FB.bin"  and parses it using the schema given in the text area "COCOL CopyBook contents"
It will generate structured records with either the output schema (if specified by the user) or with the default schema as is specified in the text area.

      {
          "name": "CopybookReader",
          "plugin": {
              "name": "CopybookReader",
              "type": "batchsource",
              "properties": {
                    "schema": "{
                        \"type\":\"record\",
                        \"name\":\"etlSchemaBody\",
                        \"fields\":[
                           {
                              \"name\":\"DTAR020-KEYCODE-NO\",
                              \"type\":\"int\"
                           },
                           {
                              \"name\":\"DATE\",
                              \"type\":[\"int\",\"null\"]
                            },
                            {
                                \"name\":\"DTAR020-DEPT-NO\",
                                \"type\":[\"int\",\"null\"]
                             },
                             {
                                \"name\":\"DTAR020-QTY-SOLD\",
                                \"type\":[\"int\",\"null\"]
                              },
                              {
                                  \"name\":\"DTAR020-SALE-PRICE\",
                                  \"type\":[\"double\",\"null\"]
                               }
                          ]
                     }",
                    "referenceName": "Copybook",
                    "copybookContents":
                        "000100* \n
                        000200* DTAR020 IS THE OUTPUT FROM DTAB020 FROM THE IML \n
                        000300* CENTRAL REPORTING SYSTEM \n
                        000400* \n
                        000500* CREATED BY BRUCE ARTHUR 19/12/90 \n
                        000600* \n
                        000700* RECORD LENGTH IS 27. \n
                        000800* \n
                        000900 03 DTAR020-KCODE-STORE-KEY. \n
                        001000 05 DTAR020-KEYCODE-NO PIC X(08). \n
                        001100 05 DTAR020-STORE-NO PIC S9(03) COMP-3. \n
                        001200 03 DTAR020-DATE PIC S9(07) COMP-3. \n
                        001300 03 DTAR020-DEPT-NO PIC S9(03) COMP-3. \n
                        001400 03 DTAR020-QTY-SOLD PIC S9(9) COMP-3. \n
                        001500 03 DTAR020-SALE-PRICE PIC S9(9)V99 COMP-3. ",

                    "binaryFilePath": "file:///home/cdap/cdap/DTAR020_FB.bin",
                    "fileStructure": ""
                    }
              }
      }

