# Copybook Reader Batch Source


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
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**copybookContents:** Contents of the COBOL copybook file which will contain the data structure.
Users will have to paste the entire contents of the copybook file into the text area. The copybook contents should be
in sync with the underlying binary data to be read. This first implementation does not handle complex nested structures
of a COBOL copybook or redefines or iterators in the structure.

**binaryFilePath:** Complete path of the .bin to be read. This will be a fixed-length binary format file that matches
the copybook. Supports compressed files using a native compressed codec. (Macro-enabled)

**drop:** Comma-separated list of fields to drop. For example: 'field1,field2,field3'.

**maxSplitSize:** Maximum split-size(MB) for each mapper in the MapReduce Job. Defaults to 1MB. (Macro-enabled)

Example
-------

This example reads data from a local binary file "file:///home/cdap/DTAR020_FB.bin"  and parses it using the schema
given in the text area "COBOL Copybook".
It will drop field "DTAR020-DATE" and generate structured records with schema as specified in the text area.

      {
          "name": "CopybookReader",
          "plugin": {
              "name": "CopybookReader",
              "type": "batchsource",
              "properties": {
                    "drop" : "DTAR020-DATE",
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
