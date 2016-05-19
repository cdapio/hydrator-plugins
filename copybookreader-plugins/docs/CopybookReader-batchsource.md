# CopybookReader plugin


Description
-----------
This is a source plugin that allows users to read and process mainframe files defined using a COBOL copybook.


Use Case
--------
It's used for reading flat files or datasets generated on either a z/OS IBM mainframe or AS/400,
based on a fixed-length COBOL copybook. If you have a flat file on HDFS that can be parsed using a simple COBOL
copybook, then applying the copybook, one is able to easily read the file and its fields.


Properties
----------
**copybookContents:** Contents of the COBOL copybook file which will contain the data structure.
Users will have to paste the entire contents of the copybook file into the text area. The copybook contents should be
in sync with the underlying binary data to be read. This first implementation does not handle complex nested structures
of a COBOL copybook or redefines or iterators in the structure.

**binaryFilePath:** Complete path of the .bin to be read. This will be a fixed-length binary format file that matches
the copybook. Supports compressed files using a native compressed codec.

**schema:** The schema for the data as it will be formatted in CDAP.

**maxSplitSize:** Maximum split-size for each mapper in the MapReduce Job. Defaults to 128MB.

Example
-------

This example reads data from a local binary file "file:///home/cdap/DTAR020_FB.bin" and parses it using the schema
given in the text area "COBOL Copybook Contents".
It will generate structured records using either the default schema or with the output schema specified by the user
in the text area.

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
                    "binaryFilePath": "file:///home/cdap/DTAR020_FB.bin",
                    "maxSplitSize": ""
                    }
              }
      }